-- dep
local json = require 'cjson'

-- gin
local Gin = require 'gin.core.gin'
local Controller = require 'gin.core.controller'
local Request = require 'gin.core.request'
local Response = require 'gin.core.response'
local Error = require 'gin.core.error'

-- app
local Routes = require 'config.routes'
local Application = require 'config.application'

-- perf
local error = error
local jencode = json.encode
local pairs = pairs
local pcall = pcall
local require = require
local setmetatable = setmetatable
--local smatch = string.match
local smatch = ngx.re.match
local function tappend(t, v) t[#t + 1] = v end

-- init Router and set routes
local Router = {}

-- response version header
local response_version_header = 'gin/' .. Gin.version

-- accept header for application
local accept_header_matcher = "^application/vnd." .. Application.name .. ".v(\\d+)(.*)+json$"

local function create_request(ngx)
    local ok, request_or_error = pcall(function() return Request.new(ngx) end)
    if ok == false then
        -- parsing errors
        local err = Error.new(request_or_error.code, request_or_error.custom_attrs)
        local response = Response.new({ status = err.status, body = err.body })
        Router.respond(ngx, response)
        return false
    end
    return request_or_error
end

-- main handler function, called from nginx
function Router.handler(ngx)
    -- add headers
--    ngx.header.content_type = 'application/json'
    ngx.header["X-Framework"] = response_version_header;

    -- create request object
    local request = create_request(ngx)
    if request == false then return end

    -- get routes
    local ok, controller_name_or_error, action, params, request = pcall(function() return Router.match(request) end)

    local response

    if ok == false then
        -- match returned an error (for instance a 412 for no header match)
        local err = Error.new(controller_name_or_error.code, controller_name_or_error.custom_attrs)
        response = Response.new({ status = err.status, body = err.body })
        Router.respond(ngx, response)
    elseif controller_name_or_error then
        -- matching routes found
        response = Router.call_controller(request, controller_name_or_error, action, params)
        Router.respond(ngx, response)
    else
        -- no matching routes found
        ngx.exit(ngx.HTTP_NOT_FOUND)
    end
end

-- match request to routes
function Router.match(request)
    local uri = request.uri
    local method = request.method

    local major_version, rest_version
    local version_source
    -- match version based on headers
    if type(request.headers['accept']) == 'string' then
        local tmpversion, err = smatch(request.headers['accept'], accept_header_matcher, 'o')
        if tmpversion then
            version_source = "header"
            major_version, rest_version = tmpversion[1], tmpversion[2]
        end
    end

    if major_version == nil then
        local tmpversion, err = smatch(uri, [[\S+/v(\d+)/\S+]], 'o')
        if tmpversion then
            version_source = "uri"
            major_version, rest_version = tmpversion[1], ''
        else
           major_version, rest_version = '1', ''
        end
    end
    local routes_dispatchers = Routes.dispatchers[tonumber(major_version)]
    if routes_dispatchers == nil then
        if version_source == "header" then
            error({ code = 102 })
        else
            error({ code = 112 })
        end
    end

    -- loop dispatchers to find route
    for i = 1, #routes_dispatchers do
        local dispatcher = routes_dispatchers[i]
        if dispatcher[method] then
            -- avoid matching if method is not defined in dispatcher
            local match = smatch(uri, dispatcher.pattern, 'o')

            --            if #match > 0 then
            if match then
                local params = match
                --                local params = {}
                --                for j = 1, #match do
                --                    if dispatcher[method].params[j] then
                --                        params[dispatcher[method].params[j]] = match[j]
                --                    else
                --                        tappend(params, match[j])
                --                    end
                --                end

                -- set version on request
                request.api_version = major_version .. rest_version
                -- return
                return major_version .. '.' .. dispatcher[method].controller, dispatcher[method].action, params, request
            end
        end
    end
    -- default routes
--    print("trigger default routes")
    local match = smatch(uri, [[^/(\w+)/v1/(?<anything>[A-Za-z0-9_]+)(/(?<name>[\w./-]+))?/?\??$]], 'o')
    if match then
        return major_version .. '.' .. match[1] .. "_controller", "entry", match, request
    end
end

local controller_bucket = setmetatable({}, {
    __index = function(t, k)
        -- load matched controller and set metatable to new instance of controller
        for _, prefix in ipairs(Routes.controllers) do
            local ok, matched_controller = pcall(function() return require(prefix .. '.' .. k) end)
--            print(matched_controller)
            if ok then
                t[k] = setmetatable(matched_controller, Controller)
                return t[k]
            end
        end
        error('could not find controller: ' .. k)
    end
})

-- call the controller
function Router.call_controller(request, controller_name, action, params)
--    -- load matched controller and set metatable to new instance of controller
--    local matched_controller = require(controller_name)
--    setmetatable(matched_controller, Controller)
    local matched_controller = controller_bucket[controller_name]
    local controller_instance = Controller.new(request, params)
    setmetatable(controller_instance, {__index = matched_controller})

    -- call action
    local ok, status_or_error, body, headers = pcall(function() return matched_controller[action](controller_instance) end)

    local response

    if ok then
        -- successful
        response = Response.new({ status = status_or_error, headers = headers, body = body })
        response.callback = controller_instance.callback
        response.opt = controller_instance.opt
    else
        -- controller raised an error
        local ok, err = pcall(function() return Error.new(status_or_error.code, status_or_error.custom_attrs) end)

        if ok then
            -- API error
            response = Response.new({ status = err.status, headers = err.headers, body = err.body })
            response.callback = controller_instance.callback
            response.opt = controller_instance.opt
        else
            -- another error, throw
            error(err)
        end
    end

    return response
end

function Router.respond(ngx, response)
    -- set status
    ngx.status = response.status
    -- set headers
    for k, v in pairs(response.headers) do
        ngx.header[k] = v
    end

    if not (response.opt and response.opt.raw) then
        -- encode body
        local json_body
        if ngx.req.get_uri_args().content_urlencoded then
            json_body = ngx.encode_args(response.body)
            ngx.header.content_type = 'application/x-www-form-urlencoded'
        else
            json_body = jencode(response.body)
            ngx.header.content_type = 'application/json'
        end
        -- ensure content-length is set
        ngx.header["Content-Length"] = ngx.header["Content-Length"] or ngx.header["content-length"] or json_body:len()
        -- print body
        ngx.print(json_body)
    else
        ngx.print(response.body)
    end

    ngx.eof()

    for i, fun in ipairs(response.callback) do
        pcall(fun)
    end
    ngx.exit(200)
end

return Router
