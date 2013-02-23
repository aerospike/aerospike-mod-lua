
local function add(a, b)
    return a + b;
end

function sum_on_match(s, bin, val)

    local function _map(rec)
        if rec[bin] == val then
             return val;
        else
            return 0;
        end
    end

    return s : map(_map) : reduce(add);
end
