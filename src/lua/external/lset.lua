-- AS Large Set (LSET) Operations

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSET Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSET module:
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) Object = get( topRec, ldtBinName, searchValue ) 
-- (*) Number = exists( topRec, ldtBinName, searchValue ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Object = take( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================
-- Reference the LMAP LDT Library Module:
local lset = require('ldt/lib_lset');

-- ======================================================================
-- || create      || (deprecated)
-- || lset_create || (deprecated)
-- ======================================================================
-- Create/Initialize a AS LSet structure in a record, using multiple bins
--
-- We will use predetermined BIN names for this initial prototype:
-- 'LSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- There can be ONLY ONE set in a record, as we are using preset fixed names
-- for the bin.
-- +========================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... |
-- +========================================================================+
-- Set Ctrl Bin is a Map -- containing control info and the list of
-- bins (each of which has a list) that we're using.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) userModule: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- ======================================================================
function create( topRec, ldtBinName, userModule )
  return lset.create( topRec, ldtBinName, userModule );
end

function lset_create( topRec, ldtBinName, userModule )
  return lset.create( topRec, ldtBinName, userModule );
end

-- ======================================================================
-- add() -- Add an object to the LSET
-- lset_insert()  :: Deprecated
-- lset_create_and_insert()  :: Deprecated
-- ======================================================================
function add( topRec, ldtBinName, newValue, userModule )
  return lset.add( topRec, ldtBinName, newValue, userModule )
end -- add()

function lset_insert( topRec, ldtBinName, newValue )
  return lset.add( topRec, ldtBinName, newValue, nil )
end -- lset_insert()

function lset_create_and_insert( topRec, ldtBinName, newValue, userModule )
  return lset.add( topRec, ldtBinName, newValue, userModule )
end -- lset_create_and_insert()

-- ======================================================================
-- add_all() -- Add a LIST of objects to the LSET.
-- lset_insert_all() :: Deprecated
-- ======================================================================
function add_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, nil );
end -- add_all()

function lset_insert_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, nil );
end

function lset_create_and_insert_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, userModule );
end

-- ======================================================================
-- get(): Return the object matching <searchValue>
-- get_with_filter() :: not currently exposed in the API
-- lset_search()
-- lset_search_then_filter()
-- ======================================================================
function get( topRec, ldtBinName, searchValue )
  return lset.get( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- get()

function get_then_filter(topRec,ldtBinName,searchValue,userModule,filter,fargs)
  return lset.get(topRec,ldtBinName,searchValue,userModule,filter,fargs);
end -- get_with_filter()

function lset_search( topRec, ldtBinName, searchValue )
  return lset.get( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- lset_search()

function
lset_search_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.get(topRec, ldtBinName, searchValue, nil, filter, fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- exists() -- return 1 if item exists, otherwise return 0.
-- exists_with_filter() :: Not currently exposed in the API
-- lset_exists() -- with and without filter
-- ======================================================================
function exists( topRec, ldtBinName, searchValue )
  return lset.exists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function exists_with_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.exists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_with_filter()

function lset_exists( topRec, ldtBinName, searchValue )
  return lset.exists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function
lset_exists_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.exists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_then_filter()

-- ======================================================================
-- scan() -- Return a list containing ALL of LSET
-- lset_scan() :: Deprecated
-- ======================================================================
function scan( topRec, ldtBinName )
  return lset.scan(topRec,ldtBinName,nil, nil, nil);
end -- scan()

function lset_scan( topRec, ldtBinName )
  return lset.scan(topRec,ldtBinName,nil, nil, nil);
end -- lset_search()

-- ======================================================================
-- filter() -- Return a list containing all of LSET that passed <filter>
-- lset_scan_then_filter() :: Deprecated
-- ======================================================================
function filter(topRec, ldtBinName, userModule, filter, fargs)
  return lset.scan(topRec, ldtBinName, userModule, filter, fargs);
end -- filter()

-- This was defined to use only predefined filter UDFs. Now Deprecated.
function lset_scan_then_filter(topRec, ldtBinName, filter, fargs)
  return lset.scan(topRec, ldtBinName, nil, filter,fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- remove() -- remove <searchValue> from the LSET
-- take() -- remove and RETURN <searchValue> from the LSET
-- lset_delete() :: Deprecated
-- Return Status (OK or error)
-- ======================================================================
function remove( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- remove()

function take( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,true );
end -- remove()

function lset_delete( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- lset_delete()

-- ======================================================================
-- remove_with_filter()
-- lset_delete_then_filter()
-- ======================================================================
function remove_with_filter( topRec, ldtBinName, searchValue, userModule,
  filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,userModule,
    filter,fargs,false);
end -- delete_then_filter()

function
lset_delete_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,filter,fargs,false);
end -- lset_delete_then_filter()

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lset_remove() :: Deprecated
-- ========================================================================
-- Completely remove this LDT: all data and the bin content.
-- If this is the LAST LDT in the record, then ALSO remove the
-- HIDDEN LDT CONTROL BIN.
-- ==>  Remove the ESR, Null out the topRec bin.  The rest will happen
-- during NSUP cleanup.
-- Parms:
-- (1) topRec: the user-level record holding the LSET Bin
-- (2) ldtBinName: The name of the LSET Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  return lset.destroy( topRec, ldtBinName );
end

function lset_remove( topRec, ldtBinName )
  return lset.destroy( topRec, ldtBinName );
end

-- ========================================================================
-- size() -- Return the number of objects in the LSET.
-- lset_size() :: Deprecated
-- ========================================================================
function size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

function get_size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

function lset_size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

-- ========================================================================
-- get_config() -- return the config settings in the form of a map
-- lset_config() -- return the config settings in the form of a map
-- ========================================================================
function get_config( topRec, ldtBinName )
  return lset.config( topRec, ldtBinName );
end

function lset_config( topRec, ldtBinName )
  return lset.config( topRec, ldtBinName );
end

-- ========================================================================
-- get_capacity() -- return the current capacity setting for this LDT.
-- set_capacity() -- set the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function get_capacity( topRec, ldtBinName )
  return lset.get_capacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  return lset.set_capacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
-- ========================================================================
--
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- (*) debug()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function dump( topRec, ldtBinName )
  return lset.dump( topRec, ldtBinName )
end

-- ========================================================================
-- debug() -- turn on/off our debug settings
-- ========================================================================
function debug( topRec, setting )
  return lset.debug( topRec, setting );
end

-- ========================================================================
--   _      _____ _____ _____ 
--  | |    /  ___|  ___|_   _|
--  | |    \ `--.| |__   | |  
--  | |     `--. \  __|  | |  
--  | |____/\__/ / |___  | |  
--  \_____/\____/\____/  \_/  (EXTERNAL)
--                            
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
