-- Large Ordered List (LLIST) Operations

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LLIST Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LLIST module:
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) List   = find( topRec, bin, value, module, filter, fargs )
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================
-- Deprecated Functions
-- (*) function create( topRec, ldtBinName, createSpec )
-- ======================================================================
-- Reference the LLIST LDT Library Module:
local llist = require('ldt/lib_llist');
-- ======================================================================

-- ======================================================================
-- create() :: Deprecated
-- ======================================================================
-- Create/Initialize a Large Ordered List  structure in a bin, using a
-- single LLIST -- bin, using User's name, but Aerospike TYPE (AS_LLIST).
--
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- (*) createSpec: The map or module that contains Create Settings
-- ======================================================================
function create( topRec, ldtBinName, createSpec )
  return llist.create( topRec, ldtBinName, createSpec );
end

-- =======================================================================
-- add() -- insert a value into the ordered list.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- (*) newValue: The value to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function add( topRec, ldtBinName, newValue, createSpec )
  return llist.add( topRec, ldtBinName, newValue, createSpec )
end

-- =======================================================================
-- add_all() -- Iterate thru the list and insert each element
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- (*) valueList: The value to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function add_all( topRec, ldtBinName, valueList, createSpec )
  return llist.add_all( topRec, ldtBinName, valueList, createSpec );
end

-- =======================================================================
-- find() -- Locate the object(s) associated with searchKey.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- (*) searchKey: The value(s) to be found
-- =======================================================================
function find( topRec, ldtBinName, searchKey )
  return llist.find( topRec, ldtBinName, searchKey );
end

-- =======================================================================
-- scan(): Return all elements
-- =======================================================================
-- Use the library find() call with no key (match all) and no filters.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- =======================================================================
function scan( topRec, ldtBinName )
  return llist.find( topRec, ldtBinName, nil, nil, nil, nil );
end

-- =======================================================================
-- filter(): Pass all elements thru the filter and return all that qualify.
-- =======================================================================
-- Use the library find() call with no key, but WITH filters.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) LdtBinName: The user's chosen name for the LDT bin
-- (*) userModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function filter( topRec, ldtBinName, userModule, filter, fargs )
  return llist.find( topRec, ldtBinName, userModule, filter, fargs );
end

-- ======================================================================
-- remove(): Remove all items corresponding to the specified key.
-- ======================================================================
-- Remove (Delete) the item(s) that correspond to "key".
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) LdtBinName
-- (3) key: The key we'll search for
-- ======================================================================
function remove( topRec, ldtBinName, key )
  return llist.remove( topRec, ldtBinName, key );
end

-- ========================================================================
-- destroy(): Remove the LDT entirely from the record.
-- ========================================================================
-- Destroy works essentially the same way for all LDTs. 
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  return llist.destroy( topRec, ldtBinName );
end -- destroy()

-- ========================================================================
-- size() -- return the number of elements (item count) in the LDT.
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function size( topRec, ldtBinName )
  return llist.size( topRec, ldtBinName );
end

-- ========================================================================
-- config() -- return the config settings
-- get_config() -- return the config settings
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
end

function get_config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
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
  return llist.get_capacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  return llist.set_capacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
-- Dump: Debugging/Tracing mechanism -- show the WHOLE tree.
-- ========================================================================
function dump( topRec, ldtBinName )
  return llist.dump( topRec, ldtBinName );
end

-- ========================================================================
-- debug() -- Turn the debug setting on (1) or off (0)
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function debug( topRec, setting )
  return llist.debug( topRec, setting );
end

-- ========================================================================
--   _      _     _____ _____ _____ 
--  | |    | |   |_   _/  ___|_   _|
--  | |    | |     | | \ `--.  | |  
--  | |    | |     | |  `--. \ | |  
--  | |____| |_____| |_/\__/ / | |  
--  \_____/\_____/\___/\____/  \_/   (EXTERNAL)
--                                  
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --

