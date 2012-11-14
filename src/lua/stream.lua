-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
-- NOTES 
--      - Currently, a Lua array (table) is being used to represent a stream.
--      - When stream is fully implemented you also need to rewrite iterate(stream).
-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

-- ######################################################################################
--
-- Utilities
--
-- ######################################################################################

--
-- Clones an array
--
local function clone(a1)
    local a2 = {}
    for i,v in ipairs(a1) do a2[i] = v end
    return a2
end

--
-- Append an operation to a StreamOps, retuning a new StreamOps with the appended operation.
--
-- @param stream_ops StreamOps to have the operation appended to.
-- @param op operation to be appended
-- @return a new StreamOps with the appended operation.
--
local function append(streamOps, op)
    local ops = clone(streamOps.ops)
    table.insert(ops, op)
    return StreamOps_create(ops)
end

-- 
-- Creates an iterator on a stream
--
-- @param stream the Stream to create an iterator from
-- @return an iterator
--
local function iterate(stream)
    local iterator = stream:iterator()
    return function()
        if ( iterator:has_next() ) then
            return iterator:next()
        else
            return nil
        end
    end
end


-- ######################################################################################
--
-- Stream Operations
--
-- ######################################################################################

local function aggregate(stream, z, f)
    local r = z
    for v in iterate(stream) do
        r = f(r,v)
    end
    return {r}
end

local function collect(stream, f)
    return aggregate(stream, nil, f)
end

local function fold(stream, z, f)
    local r = z
    for v in iterate(stream) do
        r = f(r,v)
    end
    return {r}
end

local function reduce(stream, f)
    return fold(stream, nil, f)
end

-- ######################################################################################
--
-- StreamOps
-- Builds a sequence of operations to be applied to a stream of values.
--
-- ######################################################################################

StreamOps = {}
StreamOps_mt = { __index = StreamOps }

--
-- Creates a new StreamOps using an array of ops
-- 
-- @param ops an array of operations
--
function StreamOps_create(ops)
    local inst = {}
    setmetatable(inst, StreamOps_mt);
    inst.ops = (ops or {})
    return inst
end

--
-- Evaluates the StreamOps against a Stream
-- 
-- @param stream the stream that will have ops applied to
-- @param ops the StreamOps to be applied to the stream
-- @return a stream containing the results
--
function StreamOps_eval(stream, streamOps)
    local r = stream
    for i,op in ipairs(streamOps.ops) do
        r = StreamOps_apply(op[2], r, op[3])
    end
    return r
end

-- 
-- Apply function `f` to a stream and args
--
-- @param f function to invoke
-- @param s stream to be applied to the function
-- @param a array of args to be applied to the function
-- @return result of f
--
function StreamOps_apply(f,s,a)
    local   n = #a
    if      n == 0 then return f(s)
    elseif  n == 1 then return f(s,a[1])
    elseif  n == 2 then return f(s,a[1],a[2])
    elseif  n == 3 then return f(s,a[1],a[2],a[3])
    elseif  n == 4 then return f(s,a[1],a[2],a[3],a[4])
    elseif  n == 5 then return f(s,a[1],a[2],a[3],a[4],a[5])
    elseif  n == 6 then return f(s,a[1],a[2],a[3],a[4],a[5],a[6])
    else                return nil
    end
end

function StreamOps:aggregate(z, f)
    return append(self, {'aggregate', aggregate, {z, f}})
end

function StreamOps:collect(f)
    return append(self, {'collect', collect, {f}})
end

function StreamOps:fold(z, f)
    return append(self, {'fold', fold, {z, f}})
end

function StreamOps:reduce(f)
    return append(self, {'reduce', reduce, {f}})
end
