-- perf
local error = error
local ipairs = ipairs
local pairs = pairs
local require = require
local tonumber = tonumber
local function tappend(t, v) t[#t + 1] = v end

-- settings
local timeout_subsequent_ops = 10000 -- 10 sec
local max_idle_timeout = 10000 -- 10 sec
local max_packet_size = 1024 * 1024 -- 1MB


local MySql = {}
MySql.default_database = 'mysql'

local function mysql_connect(options)
    -- ini mysql
    local mysql = require "resty.mysql"
    -- create sql object
    local db, err = mysql:new()
    if not db then error("failed to instantiate mysql: " .. err) end
    -- set 1 second timeout for suqsequent operations
    db:set_timeout(options.timeout_subsequent_ops or timeout_subsequent_ops)
    -- connect to db
    local db_options = {
        host = options.host,
        port = options.port,
        database = options.database,
        user = options.user,
        password = options.password,
        max_packet_size = options.max_packet_size or max_packet_size
    }
    local ok, err, errno, sqlstate = db:connect(db_options)
    if not ok then error("failed to connect to mysql: " .. tostring(err) .. ": " .. tostring(errno) .. " " .. tostring(sqlstate)) end
    -- return
    return db
end

local function mysql_keepalive(db, options)
    -- put it into the connection pool
    local ok, err = db:set_keepalive(options.max_idle_timeout or max_idle_timeout, options.pool)
    --    local ok, err = db:close()
    if not ok then error("failed to set mysql keepalive: " .. err) end
end

-- quote
function MySql.quote(options, str)
    return ngx.quote_sql_str(str)
end

-- return list of tables
function MySql.tables(options)
    local res = MySql.execute(options, "SHOW TABLES IN " .. options.database .. ";")
    local tables = {}

    for _, v in pairs(res) do
        for _, table_name in pairs(v) do
            tappend(tables, table_name)
        end
    end

    return tables
end

-- return schema as a table
function MySql.schema(options)
    local Migration = require 'gin.db.sql.migrations'
    local schema = {}

    local tables = MySql.tables(options)
    for i, table_name in ipairs(tables) do
        if table_name ~= Migration.migrations_table_name then
            local columns_info = MySql.execute(options, "SHOW COLUMNS IN " .. table_name .. ";")
            tappend(schema, { [table_name] = columns_info })
        end
    end

    return schema
end

-- execute query on db
local function db_execute(options, db, sql)
    local return_res = {}

    local res, err, errno, sqlstate = db:query(sql)
    return_res[#return_res + 1] = res or ngx.null
    if err ~= nil then
        ngx.log(ngx.ERR, err)
        ngx.log(ngx.ERR, sql)
    end
    if not res then
        db:close()
        error("bad mysql result: " .. tostring(err) .. ": " .. tostring(errno) .. " " .. tostring(sqlstate))
    end
    -- return
    return unpack(return_res)
end

-- execute a query
function MySql.execute(options, sql)
    -- get db object
    local db = mysql_connect(options)
    -- execute query
    local res = { db_execute(options, db, sql) }
    -- keepalive
    mysql_keepalive(db, options)
    -- return
    return unpack(res)
end

--- Execute a query and return the last ID
function MySql.execute_and_return_last_id(options, sql, id_col)
    -- get db object
    local db = mysql_connect(options)
    -- execute query
--    db_execute(options, db, sql)
--    -- get last id
--    local id_col = id_col
--    local res = db_execute(options, db, "SELECT LAST_INSERT_ID() AS " .. id_col .. ";")
    local res = db_execute(options, db, sql)
    -- keepalive
    mysql_keepalive(db, options)
--    return tonumber(res[1][id_col])
    return tonumber(res.insert_id)
end

return MySql
