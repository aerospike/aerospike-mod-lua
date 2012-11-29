
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
