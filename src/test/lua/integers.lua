
function add(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out + select(i,...)
    end
    return out
end

function sub(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out - select(i,...)
    end
    return out
end

function mult(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out * select(i,...)
    end
    return out
end

function div(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out / select(i,...)
    end
    return out
end

function pow(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out ^ select(i,...)
    end
    return out
end

function mod(r,...)
    local out = select(1,...)
    local len = select('#',...)
    for i=2, len do
        out = out % select(i,...)
    end
    return out
end

function echo(r,a)
    return a
end