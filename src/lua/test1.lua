
function record(r)
    return r.a
end


function sum(r,a,b)
    return a+b
end

function join(r,delim)
    return "join: " .. r.a .. delim .. r.b
end

function setbin(r,bin,val)
    r[bin] = val;
    aerospike:update(r);
    return r[bin];
end

function getbin(r,bin)
    return r[bin];
end

function cat(r,a,b)
    return "cat: " .. a .. b
end

function abc(r,a,b)
    log.info(r,a,b)
    return "abc"
end

function log(r,msg)
    log.info(msg)
    return 1
end

function one(r)
    return 1
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
