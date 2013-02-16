
function increment(s)
    
    local function _map(a)
        return a + 1
    end

    return s : map(_map)
end


function sum(s)

    local function _reduce(a, b)
        return a + b
    end

    return s : reduce(_reduce)
end


function product(s)
    
    local function _reduce(a, b)
        return a * b
    end

    return s : reduce(_reduce)
end

function rollup(s)

    local function _map(a)
        return map{ [a.campaign] = a.views }
    end

    local function _reduce(a, b)
        return map.merge(a, b, math.sum)
    end


    return s : map(_map) : reduce(_reduce)
end

function rollup2(s)

    local function _aggregate(a,b)
        a[b.campaign] = (a[b.campaign] or 0) + b.views
        return a
    end

    return s : aggregate(map(), _aggregate)
end