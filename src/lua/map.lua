-- ######################################################################################
--
-- Map
-- Mapping of keys to values.
--
-- ######################################################################################

local function neutral(v)
    local   t = type(v)
    if      t == 'number'   then    return 0
    elseif  t == 'string'   then    return ''
    elseif  t == 'table'    then    return {}
    elseif  t == 'boolean'  then    return false
    else                            return nil
    end
end

local function neutral_element(m)
    local p = pairs(m.elements)
    local k,v = p()
    return {neutral(k), neutral(v)}
end

local function neutral_element(list)
    return neutral(list.elements[1])
end

Map = {}
Map_mt = { __index = Map }

function Map:create(t)
    local inst = {}
    setmetatable(inst, Map_mt);
    inst.elements = t
    return inst
end

function Map:foreach(f)
    for k,v in pairs(self.elements) do
        f({k,v})
    end
    return nil
end

function Map:map(f)
    local r = {}
    for k,v in pairs(self.elements) do
        local t = f({k,v})
        r[t[1]] = t[2]
    end
    return Map:create(r)
end

function Map:flatMap(f)
    local r = {}
    for k,v in pairs(self.elements) do
        for j,t in pairs(f({k,v})) do 
            r[t[1]] = t[2]
        end
    end
    return Map.create(r)
end

function Map:aggregate(b, f)
    local r = b
    for k,v in pairs(self.elements) do
        r = f(r,{k,v})
    end
    return r
end

function Map:collect(f)
    return self:aggregate(neutral(self), f)
end

function Map:fold(a, f)
    local r = a
    for k,v in pairs(self.elements) do
        r = f(r,{k,v})
    end
    return r
end

function Map:reduce(f)
    return self:fold(neutral(self), f)
end
