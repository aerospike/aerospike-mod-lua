
-- ############################################################################
--
-- List
--
-- ############################################################################

function list.clone(l)
    local ll = {}
    for v in list.iterator(l) do
        table.insert(ll, v)
    end
    return list(ll)
end

-- ############################################################################
--
-- Map
--
-- ############################################################################

function map.merge(m1,m2,f)
    local mm = {}
    for k,v in map.pairs(m1) do
        mm[k] = v
    end
    for k,v in map.pairs(m2) do
        mm[k] = (mm[k] and f and type(f) == 'function' and f(m1[k],m2[k])) or v
    end
    return map(mm)
end

function map.diff(m1,m2)
    local mm = {}
    for k,v in map.pairs(m1) do
        if not m2[k] then
            mm[k] = v
        end
    end
    for k,v in map.pairs(m2) do
        if not m1[k] then
            mm[k] = v
        end
    end
    return map(mm)
end

function map.clone(m)
    local mm = {}
    for k,v in map.pairs(m) do
        mm[k] = v
    end
    return map(mm)
end

-- ############################################################################
--
-- Math
--
-- ############################################################################

--
-- Sum of the two values
-- 
function math.sum(a,b) 
    return a + b
end