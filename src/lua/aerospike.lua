require("stream")
require("range")

-- ######################################################################################
--
-- LOG FUNCTIONS
--
-- ######################################################################################

function trace(m, ...)
    return aerospike:log(4, string.format(m, ...))
end

function debug(m, ...)
    return aerospike:log(3, string.format(m, ...))
end

function info(m, ...)
    return aerospike:log(2, string.format(m, ...))
end

function warn(m, ...)
    return aerospike:log(1, string.format(m, ...))
end

-- ######################################################################################
--
-- APPLY FUNCTIONS
--
-- ######################################################################################

--
-- Creates a new environment for use in apply* functions
--
function env()
    local e = {}

    -- standard lua
    e["_G"] = {}
    e["setfenv"] = setfenv
    e["require"] = require
    e["pairs"] = pairs
    e["pcall"] = pcall
    e["error"] = error
    e["ipairs"] = ipairs
    e["getmetatable"] = getmetatable
    e["setmetatable"] = setmetatable
    e["print"] = print
    e['package'] = package
    e['select'] = select

    -- aerospike types
    e["record"] = record
    e["iterator"] = iterator
    e["list"] = list
    e["map"] = map
    e["aerospike"] = aerospike

    -- logging functions
    e["trace"] = trace
    e["debug"] = debug
    e["info"] = info
    e["warn"] = warn

    return e
end

--
-- Apply function to a record and arguments.
--
-- @param f the fully-qualified name of the function.
-- @param r the record to be applied to the function.
-- @param ... additional arguments to be applied to the function.
-- @return result of the called function or nil.
-- 
function apply_record(f, r, ...)

    if f == nil then
        error("function not found", 2)
    end

    setfenv(f,env())
    success, result = pcall(f, r, ...)
    if success then
        return result
    else
        error(result, 2)
        return nil
    end
end

--
-- Apply function to an iterator and arguments.
--
-- @param f the fully-qualified name of the function.
-- @param s the iterator to be applied to the function.
-- @param ... additional arguments to be applied to the function.
-- @return result of the called function or nil.
-- 

function apply_stream(f, s, ...)
    
    if f == nil then
        error("function not found", 2)
    end

    setfenv(f,env())
    success, result = pcall(f, StreamOps_create(), ...)
    if success then
        return (StreamOps_eval(s, result))[1]
    else
        error(result, 2)
        return nil
    end
end
