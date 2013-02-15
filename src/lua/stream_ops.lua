
local function check_limit(v)
    return type(v) == 'number' and v >= 1000
end

--
-- clone a table. creates a shallow copy of the table.
--
local function clone_table(t)
    local out = {}
    for k,v in pairs(t) do
        out[k] = v
    end
    return out
end

--
-- Clone a value.
--
local function clone(v)

    local t = type(v)

    if t == 'number' then
        return v
    elseif t == 'string' then
        return v
    elseif t == 'boolean' then
        return v
    elseif t == 'table' then
        return clone_table(v)
    elseif t == 'userdata' then
        local mt = getmetatable(v)
        return nil
    end

    return v
end

--
-- Filter values
-- @param next - a generator that produces the next value from a stream
-- @param f - the function to transform each value
--
function filter( next, p )
    -- done indicates if we exhausted the `next` stream
    local done = false

    -- return a closure which the caller can use to get the next value
    return function()
        
        -- we bail if we already exhausted the stream
        if done then return nil end

        -- find the first value which satisfies the predicate
        for a in next do
            if p(a) then
                return a
            end
        end

        done = true

        return nil
    end
end

--
-- Transform values
-- @param next - a generator that produces the next value from a stream
-- @param f - the tranfomation operation
--
function transform( next, f )
    -- done indicates if we exhausted the `next` stream
    local done = false

    -- return a closure which the caller can use to get the next value
    return function()
        
        -- we bail if we already exhausted the stream
        if done then return nil end
        
        -- get the first value
        local a = next()

        -- apply the transformation
        if a ~= nil then
            return f(a)
        end

        done = true;

        return nil
    end
end

--
-- Combines two values from an istream into a single value.
-- @param next - a generator that produces the next value from a stream
-- @param f - the reduction operation
--
function reduce( next, f )
    -- done indicates if we exhausted the `next` stream
    local done = false

    -- return a closure which the caller can use to get the next value
    return function()


        -- we bail if we already exhausted the stream
        if done then return nil end
        
        -- get the first value
        local a = next()


        if a ~= nil then
            -- get each subsequent value and reduce them
            for b in next do
                a = f(a,b)
            end
        end

        -- we are done!
        done = true
        
        return a
    end
end

--
-- Aggregate values into a single value.
-- @param next - a generator that produces the next value from a stream
-- @param f - the aggregation operation
--
function aggregate( next, init, f )
    -- done indicates if we exhausted the `next` stream
    local done = false

    -- return a closure which the caller can use to get the next value
    return function()

        -- we bail if we already exhausted the stream
        if done then return nil end

        -- get the initial value
        local a = clone(init)
        
        -- get each subsequent value and aggregate them
        for b in next do
            a = f(a,b)

            -- check the size limit, if it is exceeded,
            -- then return the value
            if check_limit(a) then
                return a
            end
        end

        -- we are done!
        done = true

        return a
    end
end

--
-- as_stream iterator
--
function iterator(s)
    local done = false
    return function()
        if done then return nil end
        local v = stream.read(s)
        if v == nil then
            done = true
        end
        return v;
    end
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
function StreamOps_create()
    local self = {}
    setmetatable(self, StreamOps_mt);
    self.ops = {}
    return self
end

function StreamOps_apply(stream, stream_ops, scope, i, n)
    
    -- if nil, then use default values
    scope = scope or 3
    i = i or 1
    n = n or #(stream_ops.ops)
    
    -- if index in list > size of list, then return the stream
    if i > n then return stream end
    
    -- get the current operation
    local op = stream_ops.ops[i]
    
    -- the following needs to be replaced.
    -- essentially, the server scope should be first then the client scope.
    -- While getting the server scoped ops, we should quit when we encounter the 
    -- first client scoped op.
    if op.scope == scope or op.scope == 3 then
        local s = op.func(stream, unpack(op.args)) or stream
        return StreamOps_apply(s, stream_ops, scope, i + 1, n)
    else
        local s = stream
        return StreamOps_apply(s, stream_ops, scope, i + 1, n)
    end
end



-- 
-- OPS: [ OP, ... ]
-- OP: {scope=SCOPE, name=NAME, func=FUNC, args=ARGS}
-- SCOPE: SERVER(1) | CLIENT(2) | BOTH(3)
-- NAME: FUNCTION NAME
-- FUNC: FUNCTION POINTER
-- ARGS: ARRAY OF ARGUMENTS
--

function StreamOps:aggregate(...)
    table.insert(self.ops, { scope = 1, name = "aggregate", func = aggregate, args = {...}})
    return self
end

function StreamOps:reduce(...)
    table.insert(self.ops, { scope = 3, name = "reduce", func = reduce, args = {...}})
    return self
end

function StreamOps:map(...)
    table.insert(self.ops, { scope = 3, name = "map", func = transform, args = {...}})
    return self
end

function StreamOps:filter(...)
    table.insert(self.ops, { scope = 3, name = "filter", func = filter, args = {...}})
    return self
end
