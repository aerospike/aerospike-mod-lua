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