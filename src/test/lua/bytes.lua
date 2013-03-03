
function set(r,b,l)
    r[b] = l
    if aerospike:exists(r) then
        aerospike:create(r)
    else
        aerospike:update(r)
    end

    if aerospike:exists(r) then
        info("record exists!")
    else
        info("record doesn't exist!")
    end
    return 0
end

function get(r,b)
    local l = r[b]
    if l == nil then
        return 1
    else
        return 0
    end
end

function newbytes(r,a,b,c)
    local b = bytes{1,2,3}
    info("1 => %s",b[1] or "<nil>")
    info("2 => %s",b[2] or "<nil>")
    info("3 => %s",b[3] or "<nil>")
    return l[2]
end
