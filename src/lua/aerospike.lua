require("stream")
require("range")

-- ######################################################################################
--
-- LOGGING FACILITY
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
    e["_G"] = {}
    e["setfenv"] = setfenv
    e["trace"] = trace
    e["debug"] = debug
    e["info"] = info
    e["warn"] = warn
    e["require"] = require
    e["pairs"] = pairs
    e["pcall"] = pcall
    e["error"] = error
    e["ipairs"] = ipairs
    e["getmetatable"] = getmetatable
    e["setmetatable"] = setmetatable
    e["Record"] = Record
    e["Stream"] = Stream
    e["Iterator"] = Iterator
    e["aerospike"] = aerospike
    e["print"] = print
    e['package'] = package
    e['select'] = select
    return e
end

-- 
-- Generic apply function.
-- It accepts the fully-qualified name of the function to call and arguments, then
-- loads the function and calles it.
-- 
-- @param f fully-qualified function name
-- @param ... arguments for the function
--
function apply(f, ...)

    local names = {}
    local fname = ""
    local fn = nil

    if #f <= 0 then
        error("missing function name.")
        return nil
    end

    for name in f:gmatch("%w+") do names[#names+1] = name end

    if #names <= 0 then
        error("invalid function name.")
        return nil
    end
    
    fname = names[#names]
    table.remove(names,#names)

    if #names > 0 then
        require(table.concat(names,"/"))
    end

    fn = _G[fname]

    if fn == nil then
        error("function not found: " .. f)
        return nil
    end
    
    setfenv(fn,env())

    return pcall(fn, ...)
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
    success, result = apply(f, r, ...)
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
    success, result = apply(f, StreamOps_create(), ...)
    if success then
        return (StreamOps_eval(s, result))[1]
    else
        error(result, 2)
        return nil
    end
end
