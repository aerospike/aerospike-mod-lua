
local function _join(r,delim,...)
    local out = ''
    local len = select('#',...)
    for i=1, len do
        if i > 1 then
            out = out .. (delim or ',')
        end
        out = out .. r[select(i,...)]
    end
    return out
end

function join(r,delim,...)
    return _join(r,delim,...)
end

function cat(r,...)
    return _join(r,'',...)
end

function get(r,name)
    return r[name]
end

function set(r,name,value)
    local old = r[name]
    r[name] = value
    -- aerospike.update(r)
    return old
end

function remove(r,name)
    local old = r[name]
    r[name] = nil
    -- aerospike.update(r)
    return old
end

function delete(r)
    -- return aerospike.update(r)
end
