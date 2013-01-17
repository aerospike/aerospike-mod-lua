local function _join(r,delim,...)
    local out = ''
    local len = select('#',...)
    for i=1, len do
        if i > 1 then
            out = out .. (delim or ',')
        end
        out = out .. select(i,...)
    end
    return out
end

function join(r,delim,...)
    return _join(r, delim, ...)
end

function cat(r,...)
    return _join(r, '', ...)
end

function echo(r,a)
    return a
end

function len(r, a)
    return string.len(a)
end