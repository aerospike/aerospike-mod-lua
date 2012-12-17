
function append(r,bin,...)

    local l = r[bin] or list()

    local len = select('#',...)
    for i=1, len do
        list.append(l, select(i,...))
    end

    r[bin] = l

    return l[1]
end

function prepend(r,bin,...)

    local l = r[bin] or list()

    local len = select('#',...)
    for i=1, len do
        list.prepend(l, select(i,...))
    end

    r[bin] = l

    return l[1]
end

function iterate(r,k,...)

    local l = list()

    local len = select('#',...)
    for i=1, len do
        list.append(l, select(i,...))
    end

    local j = list.iterator(l);
    while j:has_next() do
        info(j:next())
    end

    return 1
end

function lappend(r,l,...)
    local len = select('#',...)
    for i=1, len do
        list.append(l, select(i,...))
    end
    return l
end

function newlist(r,a,b,c)
    local l = list{a,b,c}
    info("1 => %s",l[1] or "<nil>")
    info("2 => %s",l[2] or "<nil>")
    info("3 => %s", l[3] or "<nil>")
    return l[2]
end
