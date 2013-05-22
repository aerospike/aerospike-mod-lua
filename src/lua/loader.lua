
-- A table to track whether we had sandboxed a function
local sandboxed = {}

--
-- load untrusted code into a "safe" environment
--
function load_safe(filename)
    local e = sandbox()
    local f = loadfile("untrusted.lua")
    local g = setfenv(f,e)
    local err, res = pcall(g)
    return err, e
end

local function create_env()

    local env;

    local function load_submodule(modname)
    end

    env = {
        print   = print,
        require = load_module
    }

    return env
end

local function loader(modname,env)
    
    local function loader_require(modname)
        local f, m = loadfile(modname .. ".lua")
        local g = setfenv(f,env)
        local s, v = pcall(g)
        if s and type(v) == 'table' then
            return v
        else
            return nil
        end
    end

    env.require = loader(env)

    
end



-- 
-- loadmodule(modname,env)
--
-- load a module with the given environment
--
-- @param modname - name of the module to load
-- @param env - environment to use for the module
-- @return the value returned from the module
--
function loadmodule(modname,env)
    local f, m = loadfile(modname .. ".lua")
    local g = setfenv(f,env)
    local s, v = pcall(g)
    if s and type(v) == 'table' then
        return v
    else
        return nil
    end
end
