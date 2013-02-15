

local function map_merge(t1,t2, merge)
    local t3 = map{}
    if t1 ~= nil then
        for k,v in pairs(t1) do
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



function sum(stream)
    
    local function _reduce(a, b)
        return a + b
    end

    return stream : reduce(_reduce)
end


function rollup(stream)

    local function _map(agg,rec)
        -- return map{ [rec.campaign] = rec.views }
        return 1
    end

    local function _reduce(agg1, agg2)
        -- return map_merge(agg1, agg2, function(v1, v2)
        --     return v1 + v2
        -- end)
        return 1
    end


    return stream : map(_map)
end