-- ######################################################################################
--
-- List
-- Builds a sequence of operations to be applied to a stream of values.
--
-- ######################################################################################

local function neutral(v)
    local t = type(v)
    if      t == 'number'   then    return 0
    elseif  t == 'string'   then    return ''
    elseif  t == 'table'    then    return {}
    elseif  t == 'boolean'  then    return false
    else                            return nil
    end
end

local function neutral_element(list)
    return neutral(list.elements[1])
end

List = {}
List_mt = { __index = List }

function List:create(a)
    local inst = {}
    setmetatable(inst, List_mt);
    inst.size = table.getn(a)
    inst.elements = a
    return inst
end

function List:foreach(f)
    for i,v in ipairs(self.elements) do
        f(v)
    end
    return nil
end

function List:map(f)
    local r = {}
    for i,v in ipairs(self.elements) do
        table.insert(r,f(v))
    end
    return List:create(r)
end

function List:flatMap(f)
    local r = {}
    for i,v in ipairs(self.elements) do
        for j,w in ipairs(f(v)) do 
            table.insert(r,w)
        end
    end
    return List:create(r)
end

function List:aggregate(b, f)
    local r = b
    for i,v in ipairs(self.elements) do
        r = f(r,v)
    end
    return r
end

function List:collect(f)
    return self:aggregate(neutral_element(self), f)
end

function List:fold(a, f)
    local r = a
    for i,v in ipairs(self.elements) do
        r = f(r,v)
    end
    return r
end

function List:reduce(f)
    return self:fold(neutral_element(self), f)
end

function List:take(n)
    local r = {}
    for i=1, n do
        table.insert(r,self.elements[i])
    end
    return List:create(r)
end

function List:drop(n)
    local r = {}
    for i=n+1, self.size do
        table.insert(r,self.elements[i])
    end
    return List:create(r)
end
