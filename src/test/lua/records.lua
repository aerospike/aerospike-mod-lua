
local function _join(r,delim,...)
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

function join(r,delim,...)
    return _join(r,delim,...)
end

function cat(r,...)
    return _join(r,'',...)
end

-- Generic example
function example_lua(r,arg1,arg2,arg3,arg4)
    r[arg1] = arg2;
    r[arg3] = arg4;
    aerospike:update(r);
    return r['b'];
end

-- Get a particular bin
function get(r,name)
    return r[name]
end

-- Set a particular bin
function set(r,name,value)
    local old = r[name]
    r[name] = value
    aerospike.update(r)
    return old
end

-- Remove a paritcular bin
function remove(r,name)
    local old = r[name]
    r[name] = nil
    aerospike.update(r)
    return old
end

-- Create a record
-- @TODO not returning error response
function create_record(r,b1,v1,b2,v2)
     local rsp = aerospike:create(r);
     if (rsp) then
 	    info("not created record already exists");
 	else 
 	    info("created");
 	end
 	r[b1] = v1;
     r[b2] = v2;
     aerospike:update(r);
 end

-- @TODO delete a record
-- function delete(r)
    -- return aerospike.update(r)
-- end

-- @TODO return record as is
-- function echo_record(record) 
--	return record;
-- end


