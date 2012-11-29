
local math = require "test_math"

-- this is not accessible, really, give it a try
global_counter = 0

-- this is accessible
local file_local_counter = 0;

function record(r)
    return r.a
end

function sum(r,a,b)
    return math.add(a,b)
end

function join(r,delim,...)
    local out = ''
    local len = select('#',...)
    for i=1, len do
        if i > 1 then
            out = out .. (delim or ',')
        end
        out = out .. r[select(i,...)]
    end
    return out
end

function setbin(r,bin,val)
    r[bin] = val;
    aerospike:update(r);
    return r[bin];
end

function getbin(r,bin)
    return r[bin];
end

function rmbin(r,bin)
    r[bin] = nil
    aerospike:update(r)
    return 1;
end

function setbins(r,b1,v1,b2,v2,b3)
    r[b1] = v1;
    r[b2] = v2;
	r[b3] = nil;
    aerospike:update(r);
    return r[b1];
end

function cat(r,a,b,c,d,e,f)
    return (a or '') .. (b or '') .. (c or '') .. (d or '') .. (e or '') .. (f or '')
end

function catfail(r,a,b,c,d,e,f)
    return a .. b .. c .. d .. e .. f
end

function abc(r,a,b)
    return "abc"
end

function log(r,msg)
    info(msg)
    return 1
end

function global_count(r)
    return global_counter
end

function local_count(r)
    return file_local_counter
end

function one(r)
    return 1
end

function make_list(r,l)
    info("@make_list")
    list.new {1,2,4}
    -- info("@make_list - l[1] = %s", l[1])
    info("@make_list - %s", l[1] or "<null>")
    info("@make_list - %d", list.size(l))
    -- info("size: %d",list.size(l))
    return list.size(l)
end

function failnil(r)
    warn('failnil')
    return r.empty
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
