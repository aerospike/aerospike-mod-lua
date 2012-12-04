
-- function create_bad_syntax(r,b1,v1,b2,v2)
--     rsp = aerospike:create(r);
--     if (rsp) {
-- 	    info("not created "..rsp);
-- 	} else { 
-- 	    info("created");
-- 	}
-- 	r[b1] = v1;
--     r[b2] = v2;
--     aerospike:update(r);
-- end

-- Should return error because binname too long
function binname_long(r,a)
  info("in binname");
  r['bin_with_a_really_long_name'] = "five";
  return aerospike:update(r);
end

-- Should return runtime error to client
function will_runtime_err(record)
  info("in binname again");
  i_dont_exist(record);
  return 0;
end

-- Should allow for many bins
function many_bins(record,count)
   aerospike:create(record);
   for i = 1, count do
      record[i] = i
   end
   aerospike:update(record);
   return 0;
end	

function set_maplist_bins(record,a,b)
    --  map{k1="v1",k2="v2"};
    -- record[a] = map();
    -- record[a]["k1"] = "v1";
    -- record[a]["k2"] = "v2";
    info("set_maplist_bins");
    local m = map();
    m["k1"] = "v1";
    m["k2"] = "v2";
    record[a] = m;
    local l = list();
    l[0] = "l1";
    l[1] = "l2";    
    record[b] = l;
    aerospike:update(record);
	return "OK"
end

function get_map_bin(record,a)
    local m = record[a];
    return m["k2"];
end

function get_list_bin(record,a)
    local l = record[a];
    return l[1];
end


