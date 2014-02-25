-- Large Stack Object (LSTACK) Operations.
-- Track the data and iteration of the last update.

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSTACK Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSTACK module:
--
-- (*) Status = push( topRec, ldtBinName, newValue, userModule )
-- (*) Status = push_all( topRec, ldtBinName, valueList, userModule )
-- (*) List   = peek( topRec, ldtBinName, peekCount ) 
-- (*) List   = pop( topRec, ldtBinName, popCount ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, peekCount,userModule,filter,fargs)
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
--
-- Reference the LSTACK LDT Library Module
local lstack = require('ldt/lib_lstack');

-- ======================================================================
-- || create        ||
-- || lstack_create ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
--
-- Parms
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- (3) createSpec: The map (not list) of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function create( topRec, ldtBinName, createSpec )
  return lstack.create( topRec, ldtBinName, createSpec );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_create( topRec, ldtBinName, createSpec )
  return lstack.create( topRec, ldtBinName, createSpec );
end

-- =======================================================================
-- push()
-- lstack_push()
-- =======================================================================
-- Push a value on the stack, with the optional parm to set the LDT
-- configuration in case we have to create the LDT before calling the push.
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec )
end -- push()

function create_and_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec );
end -- create_and_push()

-- OLD EXTERNAL FUNCTIONS
function lstack_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec )
end -- end lstack_push()

function lstack_create_and_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec );
end -- lstack_create_and_push()

-- =======================================================================
-- Stack Push ALL
-- =======================================================================
-- Iterate thru the list and call localStackPush on each element
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push_all( topRec, ldtBinName, valueList, createSpec )
  return lstack.push_all( topRec, ldtBinName, valueList, createSpec )
end

-- OLD EXTERNAL FUNCTIONS
function lstack_push_all( topRec, ldtBinName, valueList, createSpec )
  return lstack.push_all( topRec, ldtBinName, valueList, createSpec )
end

-- =======================================================================
-- peek() -- with and without filters
-- lstack_peek() -- with and without filters
--
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function peek( topRec, ldtBinName, peekCount )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, nil, nil )
end -- peek()

function filter( topRec, ldtBinName, peekCount, userModule, filter, fargs )
  return lstack.peek(topRec,ldtBinName,peekCount,userModule,filter,fargs );
end -- peek_then_filter()

-- OLD EXTERNAL FUNCTIONS (didn't have userModule in the first version)
function lstack_peek( topRec, ldtBinName, peekCount )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, nil, nil )
end -- lstack_peek()

-- OLD EXTERNAL FUNCTIONS (didn't have userModule in the first version)
function lstack_peek_then_filter( topRec, ldtBinName, peekCount, filter, fargs )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, filter, fargs );
end -- lstack_peek_then_filter()

-- ========================================================================
-- size() -- return the number of elements (item count) in the stack.
-- get_size() -- return the number of elements (item count) in the stack.
-- lstack_size() -- return the number of elements (item count) in the stack.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the size)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function size()

function get_size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function get_size()

-- OLD EXTERNAL FUNCTIONS
function lstack_size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function get_size()

-- ========================================================================
-- get_capacity() -- return the current capacity setting for LSTACK.
-- lstack_get_capacity() -- return the current capacity setting for LSTACK.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function get_capacity( topRec, ldtBinName )
  return lstack.get_capacity( topRec, ldtBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_get_capacity( topRec, ldtBinName )
  return lstack.get_capacity( topRec, ldtBinName );
end

-- ========================================================================
-- config() -- return the lstack config settings.
-- get_config() -- return the lstack config settings.
-- lstack_get_config() -- return the lstack config settings.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   res = (when successful) config Map 
--   res = (when error) nil
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

function get_config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lstack_remove() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Question  -- Reset the record[ldtBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) binName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function destroy( topRec, ldtBinName )
  return lstack.destroy( topRec, ldtBinName );
end -- destroy()

-- OLD EXTERNAL FUNCTIONS
function lstack_remove( topRec, ldtBinName )
  return lstack.destroy( topRec, ldtBinName );
end -- lstack_remove()
-- ========================================================================
-- lstack_set_storage_limit()
-- lstack_set_capacity()
-- set_storage_limit()
-- set_capacity()
-- ========================================================================
-- This is a special command to both set the new storage limit.  It does
-- NOT release storage, however.  That is done either lazily after a 
-- warm/cold insert or with an explit lstack_trim() command.
-- Parms:
-- (*) topRec: the user-level record holding the LSO Bin
-- (*) ldtBinName: The name of the LSO Bin
-- (*) newLimit: The new limit of the number of entries
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function lstack_set_capacity( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

function set_capacity( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_set_storage_limit( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

function set_storage_limit( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

-- ========================================================================
-- one()          -- Just return 1.  This is used for perf measurement.
-- same()         -- Return Val parm.  Used for perf measurement.
-- ========================================================================
-- Do the minimal amount of work -- just return a number so that we
-- can measure the overhead of the LDT/UDF infrastructure.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) Val:  Random number val (or nothing)
-- Result:
--   res = 1 or val
-- ========================================================================
function one( topRec, ldtBinName )
  return 1;
end

function same( topRec, ldtBinName, val )
  if( val == nil or type(val) ~= "number") then
    return 1;
  else
    return val;
  end
end

-- ========================================================================
-- lstack_debug() -- Turn the debug setting on (1) or off (0)
-- debug()        -- Turn the debug setting on (1) or off (0)
-- one()          -- Just return 1.  This is used for perf measurement.
-- same()         -- Return Val parm.  Used for perf measurement.
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- It would be nice if we could figure out how to make this setting change
-- PERSISTENT. Until we do that, this will be a no-op.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function debug( topRec, setting )
  return lstack.debug( topRec, setting );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_debug( topRec, setting )
  return lstack.debug( topRec, setting );
end

-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
--   _      _____ _____ ___  _____  _   __
--  | |    /  ___|_   _/ _ \/  __ \| | / /
--  | |    \ `--.  | |/ /_\ \ /  \/| |/ / 
--  | |     `--. \ | ||  _  | |    |    \ 
--  | |____/\__/ / | || | | | \__/\| |\  \
--  \_____/\____/  \_/\_| |_/\____/\_| \_/   (EXTERNAL)
--                                        
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
--
