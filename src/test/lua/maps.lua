function new(r,bin,...)
    local m = map()
    local len = select('#',...)
    
    if len % 2 > 0 then error("odd number of elements") end

    for i=1, len do
        if i % 2 == 1 then
            local k = select(i,...)
            local v = select(i+1,...)
            m[k] = v
        end
    end

    r[bin] = m

    return r[bin]
end

function set(r,k,v)
    local m = map()
    m[k] = v
    return m[k]
end

function get(r,bin,key)
    local m = r[bin]
    return m[key]
end

function newmap(r,bin,a,b,c)
    local m = map {a=a,b=b,c=c}
    info("%s => %s", "a", m.a or "<nil>")
    info("%s => %s", "b", m.b or "<nil>")
    info("%s => %s", "b", m.c or "<nil>")
    return m["b"]
end

function putmap(r,bin,m)
    r[bin] = m
    info("%s => %s", "A", m['A'] or "<nil>")
    info("%s => %s", "B", m['B']  or "<nil>")
    info("%s => %s", "C", m['C']  or "<nil>")
    aerospike:create(r);
    return r[bin]
end

function getmap(r,bin)
    info("%s => %s", "A", r[bin]['A'] or "<nil>")
    return r[bin]
end


function mapput(r, map, k, v) 
    map[k] = v
    return map
end

function show(r, map, k) 
    info("show: %s",map[k]);
    return map;
end
