
function record(r,a,b,c)
    return "hello record " .. a .. " " .. b .. " " .. c .. " END"
end

local function f1(b,a)
    b = b or {}
    b["sum"] = (b["sum"] or 0) + a
    return b
end

local function f2(a,b)
    return (a or 0)+b
end

function stream(s)
    return s : reduce(f2)
end
