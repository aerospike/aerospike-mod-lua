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
    return {

        -- aerospike types
        ["record"] = record,
        ["iterator"] = iterator,
        ["list"] = list,
        ["map"] = map,
        ["aerospike"] = aerospike,

        -- logging functions
        ["trace"] = trace,
        ["debug"] = debug,
        ["info"] = info,
        ["warn"] = warn,
        
        -- standard lua functions
        ["error"] = error,
        ["getmetatable"] = getmetatable,
        ["ipairs"] = ipairs,
        ["load"] = load,
        ["module"] = module,
        ["next"] = next,
        ["pairs"] = pairs,
        ["print"] = print,
        ["pcall"] = pcall,
        ["rawequal"] = rawequal,
        ["rawget"] = rawget,
        ["rawset"] = rawset,
        ["require"] = require,
        ["require"] = require,
        ["select"] = select,
        ["setmetatable"] = setmetatable,
        ["setfenv"] = setfenv,
        ["tonumber"] = tonumber,
        ["tostring"] = tostring,
        ["type"] = type,
        ["unpack"] = unpack,
        ["xpcall"] = xpcall,

        -- standard lua objects
        ["math"] = math,
        ["io"] = io,
        ["os"] = {
            ['clock'] = os.clock,
            ['date'] = os.date,
            ['difftime'] = os.difftime,
            ['getenv'] = os.getenv,
            ['setlocale'] = os.setlocale,
            ['time'] = os.time,
            ['tmpname'] = os.tmpname
        },
        ["package"] = package,
        ["string"] = string,
        ["table"] = table,

        -- standard lua variables
        ["_G"] = {}
    }
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
