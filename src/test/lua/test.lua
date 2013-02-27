package.path = package.path .. ";" .. "/Users/chris/projects/cstivers78/aerospike-mod-lua/src/lua/?.lua;"

-- require('stream')


--
-- Merge two tables. If the keys in the two tables match, then
-- call `merge()` to merge to the values.
--
function table_merge(t1,t2, merge)
    local t3 = {}
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

--
-- Generate a range of numbers
--
function range(start,limit)
    local i = start
    return function()
        if  i < limit then
            i = i + 1
            return i
        else
            return nil
        end
    end
end

--
-- Generate a values using a function
--
function generate(limit, fn)
    local i = 0
    return function()
        if  i < limit then
            return fn()
        else
            return nil
        end
    end
end

--
-- Iterates over values in an array
--
function iterator(t)
    local i = 0
    local n = table.getn(t)
    return function()
        i = i + 1
        if i <= n then return t[i] end
    end
end


function log(test, desc, stream, stringify)
    stringify = stringify or tostring
    print(string.format("   [%s] %s", test, desc))
    for v in stream do
        print(string.format("        - %s", stringify(v)))
    end
end

function test_integer_sum(i,n)

    local function _map(a)
        return a 
    end

    local function _aggregate(a,b)
        return a + (b or 0)
    end

    local function _reduce(a,b)
        return (a or 0) + (b or 0)
    end

    -- ########################################################################

    print("")
    print(string.format("TEST: SUM INTEGERS IN RANGE (%d,%d)",i,n))
    print("")
    print("   expected: 5050")
    print("")

    local t1 = reduce( range(i,n), _reduce )
    log("t1", "reduce(s)", t1)

    local t2 = aggregate( range(i,n), 0,  _aggregate )
    log("t2", "aggregate(s)", t2)

    local t3 = reduce( aggregate( range(i,n), 0,  _aggregate ), _reduce )
    log("t3", "reduce(aggregate(s))", t3)

    local s4 = StreamOps.create() : map(_map) : reduce(_reduce)
    local t4 = StreamOps.apply( range(i,n), s4, 1)
    log("t4", "s : map : reduce", t4)

    local s5 = StreamOps.create() : aggregate(0, _aggregate) : reduce(_reduce)
    local t5 = StreamOps.apply( range(i,n), s5, 1)
    log("t5", "s : aggregate : reduce", t5)
    

    print("")
end






function test_rollup()

    local campaign_views = {
        { campaign = "a", views = 1 },
        { campaign = "b", views = 2 },
        { campaign = "c", views = 2 },
        { campaign = "a", views = 2 },
        { campaign = "b", views = 4 },
        { campaign = "c", views = 6 },
        { campaign = "a", views = 3 },
        { campaign = "b", views = 6 },
        { campaign = "c", views = 9 },
        { campaign = "a", views = 4 },
        { campaign = "b", views = 8 },
        { campaign = "c", views = 12 },
        { campaign = "a", views = 5 },
        { campaign = "b", views = 10 },
        { campaign = "c", views = 15 },
        { campaign = "a", views = 6 },
        { campaign = "b", views = 12 },
        { campaign = "c", views = 18 }
    }

    local function result_tostring(t)
        local s = "{"
        local e = false
        for k,v in pairs(t) do 
            if e then
                s = s .. ", "
            end
            s = s .. string.format(" %s = %d", k, v)
            e = true
        end
        s = s .. " }"
        return s
    end

    -- ########################################################################

    local function _map(rec)
        return { [rec.campaign] = rec.views }
    end

    local function _aggregate(agg,rec)
        agg[rec.campaign] = (agg[rec.campaign] or 0) + rec.views
        return agg
    end

    local function _reduce(agg1, agg2)
        return table_merge(agg1, agg2, function(v1, v2)
            return v1 + v2
        end)
    end

    -- ########################################################################

    print("")
    print(string.format("TEST: ROLLUP AD IMPRESSIONS"))
    print("")
    print("   expected: { a = 21,  c = 62,  b = 42 }")
    print("")

    local t1 = aggregate( iterator(campaign_views), {},  _aggregate )
    log("t1", "aggregate(s)", t1, result_tostring)

    local t2 = reduce( aggregate( iterator(campaign_views), {},  _aggregate ), _reduce )
    log("t2", "reduce(aggregate(s))", t2, result_tostring)

    local s3 = StreamOps.create() : aggregate({}, _aggregate) : reduce(_reduce)
    local t3 = StreamOps.apply( iterator(campaign_views), s3, 1)
    log("t3", "s : aggregate : reduce", t3, result_tostring)

    local s4 = StreamOps.create() : map(_map) : reduce(_reduce)
    local t4 = StreamOps.apply( iterator(campaign_views), s4, 1)
    log("t4", "s : map : reduce", t4, result_tostring)

    print("")
end




-- test_integer_sum(0,100)
-- test_rollup()



