

local function map_merge(t1,t2, merge)
    local t3 = map()
    if t1 ~= nil then
        for k, v in pairs(t1) do
            t3[k] = v
        end
    end
    if t2 ~= nil then
        for k, v in pairs(t2) do
            if t3[k] then
                t3[k] = merge(t3[k], t2[k])
            else
                t3[k] = v
            end
        end
    end
    return t3
end



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
        -- return 1
    end

    local function _reduce(a, b)
        return a
        -- return map_merge(a, b, function(av, bv)
        --     return av + bv
        -- end)
    end


    return s : map(_map) : reduce(_reduce)
end