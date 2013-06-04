-- AS Large Set (LSET) Operations
-- Steelman (Last Update May 15, 2013: tjl)
--
-- Keep this in sync with the version above.
local MOD="LsetSteelman_05.15.0"; -- the module name used for tracing

-- Please refer to lset_design.lua for architecture and design notes.
--
-- Aerospike Large Set (LSET) Operations
-- (*) lset_create():
--          :: Create the LSET object in the bin specified, using the
--          :: creation arguments passed in (probably a package).
-- (*) lset_create_and_insert():
--          :: Insert an item into the Large Set, Create first if needed.
--          :: Apply the creation package on create.
-- (*) lset_insert(): Insert an item into the Large Set
-- (*) lset_search(): Search for an item in the Large Set
-- (*) lset_search_then_filter(): Search for an item in the set
--          :: with an additional search filter (for complex objects).
-- (*) lset_exists(): Test Existence on an item in the set
-- (*) lset_exists_then_filter(): Test Existence on an item in the set
--          :: with an additional search filter (for complex objects)
-- (*) lset_delete(): Delete an item from the set
-- (*) lset_delete_then_filter(): QUESTION:: DO WE NEED THIS????
-- (*) lset_config(): retrieve all current config settings in map format
-- (*) lset_size():   Return the size (e.g. item count) of the Set

-- ======================================================================
-- TO DO List:
-- ======================================================================
-- TODO: (*) Switch to Return Codes and ResultList (API)
-- Done: (*) Do Parameter validation for all external calls.
-- Done: (*) Add lset_create_and_insert(), which is insert plus the
--           creation parms (in case we have to create).
-- Done: (*) Verity that all serious errors call error('msg') rather than
--           just return.
-- Done: (*) Verify order of operations so that the upper level record
--           is NEVER written before all of the lower level ops have
--           successfully complete.
-- ======================================================================
--
-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- ======================================================================
local GP=true; -- Leave this set to true.
local F=true; -- Set F (flag) to true to turn ON global print

-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
-- This flavor of LDT
local LDT_LSET   = "LSET";

-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- In this early version of SET, we distribute values among lists that we
-- keep in the top record.  This is the default modulo value for that list
-- distribution.   Later we'll switch to a more robust B+ Tree version.
local DEFAULT_DISTRIB = 31;
-- Switch from a single list to distributed lists after this amount
local DEFAULT_THRESHHOLD = 100;

-- Use this to test for CtrlMap Integrity.  Every map should have one.
local MAGIC="MAGIC";     -- the magic value for Testing LSET integrity

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY  ='B'; -- Using a Transform function to compact values
local SM_LIST    ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
local KT_ATOMIC  ='A'; -- the set value is just atomic (number or string)
local KT_COMPLEX ='C'; -- the set value is complex. Use Function to get key.

-- Key Compare Function for Complex Objects
-- By default, a complex object will have a "KEY" field, which the
-- key_compare() function will use to compare.  If the user passes in
-- something else, then we'll use THAT to perform the compare, which
-- MUST return -1, 0 or 1 for A < B, A == B, A > B.
-- UNLESS we are using a simple true/false equals compare.
-- ========================================================================
-- Actually -- the default will be EQUALS.  The >=< functions will be used
-- in the Ordered LIST implementation, not in the simple list implementation.
-- ========================================================================
local KC_DEFAULT="keyCompareEqual"; -- Key Compare used only in complex mode
local KH_DEFAULT="keyHash";         -- Key Hash used only in complex mode

-- AS LSET Bin Names
local LSET_CONTROL_BIN       = "LSetCtrlBin";
local LSET_DATA_BIN_PREFIX   = "LSetBin_";

-- ++===============++
-- || Package Names ||
-- ++===============++
-- Specific Customer Names (to be moved out of the System Table)
local PackageStumbleUpon     = "StumbleUpon";

-- Standard, Test and Debug Packages
local PackageStandardList    = "StandardList";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
local PackageDebugModeList   = "DebugModeList";
local PackageDebugModeBinary = "DebugModeBinary";
local PackageDebugModeNumber = "DebugModeNumber";

-- set up our "outside" links
local  CRC32 = require('CRC32');
local functionTable = require('UdfFunctionTable');

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
local function packageStandardList( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_ATOMIC; -- Atomic Keys
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted

end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_ATOMIC; -- Atomic Keys
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = "compressTest4";
  lsetCtrlMap.UnTransform = "unCompressTest4";
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted

end -- packageTestModeBinary( lsetCtrlMap )

-- ======================================================================
-- Package = "StumbleUpon"
-- StumbleUpon uses a compacted representation.
-- NOTE: This will eventually move to the UDF Function Table, or to a
-- separate Configuration file.  For the moment it is included here for
-- convenience. 
-- ======================================================================
local function packageStumbleUpon( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = "compress4ByteInteger";
  lsetCtrlMap.UnTransform = "unCompress4ByteInteger";
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_BINARY; -- Use a Byte Array
  lsetCtrlMap.BinaryStoreSize = 4; -- Storing a single 4 byte integer
  lsetCtrlMap.KeyType = KT_ATOMIC; -- Atomic Keys (a number)
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 100; -- Rehash after this many have been inserted
  
end -- packageStumbleUpon( lsetCtrlMap )

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSET with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_ATOMIC; -- Atomic Keys
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 4; -- Rehash after this many have been inserted

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( lsetCtrlMap )
  
  -- General Parameters
  lsetCtrlMap.Transform = "compressTest4";
  lsetCtrlMap.UnTransform = "unCompressTest4";
  lsetCtrlMap.KeyCompare = "debugListCompareEqual"; -- "Simple" list comp
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = 16; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_COMPLEX; -- special function for list compare.
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 4; -- Rehash after this many have been inserted

end -- packageDebugModeBinary( lsetCtrlMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( lsetCtrlMap )
  local meth = "packageDebugModeNumber()";
  GP=F and trace("[ENTER]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring(lsetCtrlMap) );
  
  -- General Parameters
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.KeyCompare = nil;
  lsetCtrlMap.StoreState = SS_COMPACT; -- start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = 0; -- Don't waste room if we're not using it
  lsetCtrlMap.KeyType = KT_ATOMIC; -- Simple Number (atomic) compare
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 4; -- Rehash after this many have been inserted

  GP=F and trace("[EXIT]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring(lsetCtrlMap) );
end -- packageDebugModeNumber( lsetCtrlMap )

-- ======================================================================
-- adjustLSetMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LSetMap.
-- Parms:
-- (*) lsetCtrlMap: the main LSET Bin value
-- (*) argListMap: Map of LSET Settings 
-- ======================================================================
local function adjustLSetMap( lsetCtrlMap, argListMap )
  local meth = "adjustLSetMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsetMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(lsetCtrlMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the create() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

  for name, value in map.pairs( argListMap ) do
    GP=F and trace("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s)",
        MOD, meth, tostring( name ), tostring( value ));

    -- Process our "prepackaged" settings first:
    -- NOTE: Eventually, these "packages" will be installed in either
    -- a separate "package" lua file, or possibly in the UdfFunctionTable.
    -- Regardless though -- they will move out of this main file, except
    -- maybe for the "standard" packages.
    if name == "Package" and type( value ) == "string" then
      -- Figure out WHICH package we're going to deploy:
      if value == PackageStandardList then
          packageStandardList( lsetCtrlMap );
      elseif value == PackageTestModeList then
          packageTestModeList( lsetCtrlMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( lsetCtrlMap );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( lsetCtrlMap );
      elseif value == PackageStumbleUpon then
          packageStumbleUpon( lsetCtrlMap );
      elseif value == PackageDebugModeList then
          packageDebugModeList( lsetCtrlMap );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( lsetCtrlMap );
      elseif value == PackageDebugModeNumber then
          packageDebugModeNumber( lsetCtrlMap );
      end
    elseif name == "KeyType" and type( value ) == "string" then
      -- Use only valid values (default to ATOMIC if not specifically complex)
      -- Allow both upper and lower case versions of "complex".
      if value == KT_COMPLEX or value == "complex" then
        lsetCtrlMap.KeyType = KT_COMPLEX;
      else
        lsetCtrlMap.KeyType = KT_ATOMIC; -- this is the default.
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        lsetCtrlMap.StoreMode = value;
      end
    elseif name == "Modulo"  and type( value ) == "number" then
      -- Verify it's a valid value
      if value > 0 and value < MODULO_MAX then
        lsetCtrlMap.Modulo = value;
      end
    end
  end -- for each argument

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(lsetCtrlMap));
      
  return lsetCtrlMap
end -- adjustLSetMap


-- ======================================================================
-- initializeLSetMap:
-- ======================================================================
-- Set up the LSetMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSetBIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSet
-- behavior.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) namespace: The Namespace of the record (topRec)
-- (*) set: The Set of the record (topRec)
-- (*) lsetBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized lsetCtrlMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLSetMap(topRec, lsetBinName )
  local meth = "initializeLSetMap()";
  GP=F and trace("[ENTER]:<%s:%s>::Bin(%s)",MOD, meth, tostring(lsetBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsetCtrlMap = map();
  -- General Parameters
  lsetCtrlMap.LdtType = LDT_LSET; -- Mark this as a Large Set
  lsetCtrlMap.Magic = MAGIC;
  lsetCtrlMap.ExistSubRecDig = 0; -- Pt to the LDT "Exists" subrecord (digest)
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.KeyCompare = nil; -- Key Func used only in complex mode
  lsetCtrlMap.StoreState = SS_COMPACT; -- always start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = 0; -- only used in "binary" StoreMode.
  lsetCtrlMap.KeyType = KT_ATOMIC; -- assume "atomic" values for now.
  -- NOTE: We are still hard-coding the Bin Name:
  -- lsetCtrlMap.BinName = lsetBinName;
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.ItemCount = 0;   -- Count of valid elements
  lsetCtrlMap.TotalCount = 0;  -- Count of both valid and deleted elements
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 101; -- Rehash after this many have been inserted

  GP=F and trace("[ENTER]: <%s:%s>:: lsetCtrlMap(%s)",
                 MOD, meth, tostring(lsetCtrlMap));

  return lsetCtrlMap;
end -- initializeLSetMap()

-- ======================================================================
-- local function lsetSummary( lsetMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsetMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsetSummary( lsetMap )
  if lsetMap == nil or lsetMap.Magic ~= MAGIC then return "BROKEN MAP"; end;

  local resultMap                = map();
  resultMap.SUMMARY              = "Large Set Summary";
  resultMap.LdtType              = lsetMap.LdtType;
  resultMap.BinName              = lsetMap.BinName;
  resultMap.ItemCount            = lsetMap.ItemCount;
  resultMap.TotalCount           = lsetMap.TotalCount;
  resultMap.Modulo               = lsetMap.Modulo;
  resultMap.ThreshHold           = lsetMap.ThreshHold;
  resultMap.StoreMode            = lsetMap.StoreMode;
  resultMap.StoreState           = lsetMap.StoreState;
  resultMap.Transform            = lsetMap.Transform;
  resultMap.UnTransform          = lsetMap.UnTransform;
  resultMap.KeyCompare           = lsetMap.KeyCompare;
  resultMap.BinaryStoreSize      = lsetMap.BinaryStoreSize;
  resultMap.KeyType              = lsetMap.KeyType;

return resultMap;
end -- lsetSummary()

-- ======================================================================
-- local function lsetSummaryString( lsetMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsetMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsetSummaryString( lsetMap )
  return tostring( lsetSummary( lsetMap ));
end -- lsetSummaryString()

-- ======================================================================
-- We use the "CRC32" package for hashing the value in order to distribute
-- the value to the appropriate "sub lists".
-- ======================================================================
-- local  CRC32 = require('CRC32'); Do this above, in the "global" area
-- ======================================================================
-- Return the hash of "value", with modulo.
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- ======================================================================
local function stringHash( value, modulo )
  if value ~= nil and type(value) == "string" then
    return CRC32.Hash( value ) % modulo;
  else
    return 0;
  end
end -- stringHash

-- ======================================================================
-- Return the hash of "value", with modulo
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- NOTE: Use a better Hash Function.
-- ======================================================================
local function numberHash( value, modulo )
  local meth = "numberHash()";
  local result = 0;
  if value ~= nil and type(value) == "number" then
    -- math.randomseed( value ); return math.random( modulo );
    result = CRC32.Hash( value ) % modulo;
  end
  GP=F and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
  return result
end -- numberHash

-- ======================================================================
-- Get (create) a unique bin name given the current counter.
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- ======================================================================
local function getBinName( number )
  local binPrefix = "LSetBin_";
  return binPrefix .. tostring( number );
end

-- ======================================================================
-- setupNewBin: Initialize a new bin -- (the thing that holds a list
-- of user values).
-- Parms:
-- (*) topRec
-- (*) Bin Number
-- Return: New Bin Name
-- ======================================================================
local function setupNewBin( topRec, binNum )
  local meth = "setupNewBin()";
  GP=F and trace("[ENTER]: <%s:%s> Bin(%d) ", MOD, meth, binNum );

  local binName = getBinName( binNum );
  topRec[binName] = list(); -- Create a new list for this new bin

  GP=F and trace("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)",
                 MOD, meth, binNum, binName );

  return binName;
end -- setupNewBin

-- ======================================================================
-- computeSetBin()
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single bin.
-- Second -- use the right hash function (depending on the type).
-- And, know if it's an atomic type or complex type.
-- ======================================================================
local function computeSetBin( newValue, lsetCtrlMap )
  local meth = "computeSetBin()";
  GP=F and trace("[ENTER]: <%s:%s> val(%s) Map(%s) ",
                 MOD, meth, tostring(newValue), tostring(lsetCtrlMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local binNumber  = 0;
  if lsetCtrlMap.StoreState == SS_COMPACT then
    return 0
  else
    if type(newValue) == "number" then
      binNumber  = numberHash( newValue, lsetCtrlMap.Modulo );
    elseif type(newValue) == "string" then
      binNumber  = stringHash( newValue, lsetCtrlMap.Modulo );
    elseif type(newValue) == "userdata" then
      -- We are assuming that the user has supplied a function for us to
      -- deal with a complex object.  If no function, then error.
      -- Note that the easy case is the keyHashFunction(), which is a
      -- hash on a field called "KEY".

      -- TODO: Fix this
      print("COMPUTE SET BIN::MUST USE EXTRACT FUNCTION HERE!!!");

      print("MUST REGISTER A HASH FUNCTION FOR COMPLEX TYPES!!");

      binNumber  = stringHash( newValue.KEY, lsetCtrlMap.Modulo );
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type (should be number, string or map)",
           MOD, meth );
      error('ERROR: Incorrect Type for new Large Set value');
    end
  end
  GP=F and trace("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) ",
                 MOD, meth, tostring(newValue), binNumber );

  return binNumber;
end -- computeSetBin()

-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the lsetCtrlMap, if present, and apply
-- it to the value, returning the transformed value.  If no transform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyTransform( transformFunc, newValue )
  local meth = "applyTransform()";
  GP=F and trace("[ENTER]: <%s:%s> transform(%s) type(%s) Value(%s)",
 MOD, meth, tostring(transformFunc), type(transformFunc), tostring(newValue));

  local storeValue = newValue;
  if transformFunc ~= nil then 
    storeValue = transformFunc( newValue );
  end
  return storeValue;
end -- applyTransform()

-- =======================================================================
-- Apply UnTransform Function
-- Take the UnTransform defined in the lsetCtrlMap, if present, and apply
-- it to the dbValue, returning the unTransformed value.  If no unTransform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyUnTransform( lsetCtrlMap, storeValue )
  local returnValue = storeValue;
  if lsetCtrlMap.UnTransform ~= nil and
    functionTable[lsetCtrlMap.UnTransform] ~= nil then
    returnValue = functionTable[lsetCtrlMap.UnTransform]( storeValue );
  end
  return returnValue;
end -- applyUnTransform( value )

-- =======================================================================
-- unTransformSimpleCompare()
-- Apply the unTransform function and compare the values.
-- Return the unTransformed search value if the values match.
-- =======================================================================
local function unTransformSimpleCompare(unTransform, dbValue, searchValue)
  local modSearchValue = searchValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modSearchValue = unTransform( searchValue );
  end

  if dbValue == modSearchValue then
    resultValue = modSearchValue;
  end

  return resultValue;
end -- unTransformSimpleCompare()


-- =======================================================================
-- unTransformComplexCompare()
-- Apply the unTransform function and compare the values, using the
-- compare function (it's a complex compare).
-- Return the unTransformed search value if the values match.
-- parms:
-- (*) trans: The transformation function: Perform if not null
-- (*) comp: The Compare Function (must not be null)
-- (*) dbValue: The value pulled from the DB
-- (*) searchValue: The value we're looking for.
-- =======================================================================
local function unTransformComplexCompare(trans, comp, dbValue, searchValue)
  local modSearchValue = searchValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modSearchValue = unTransform( searchValue );
  end

  if dbValue == modSearchValue then
    resultValue = modSearchValue;
  end

  return resultValue;
end -- unTransformComplexCompare()


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is COMPLEX SCAN, which means we are comparing the KEY field of the
-- map object in both the value and in the List.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) binList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For insert (FV_INSERT):
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanList(lsetCtrlMap, binList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;

  local transform = nil;
  local unTransform = nil;
  if lsetCtrlMap.Transform ~= nil then
    transform = functionTable[lsetCtrlMap.Transform];
  end

  if lsetCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[lsetCtrlMap.UnTransform];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
      resultValue = unTransformComplexCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          binList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = lsetCtrlMap.ItemCount;
          lsetCtrlMap.ItemCount = itemCount - 1;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result
        return resultValue;
      end -- end if found it
    end -- end if value not nil or empty
  end -- for each list entry in this binList

  -- Didn't find it.  If FV_INSERT, then append the value to the list
  if flag == FV_INSERT then
    GP=F and trace("[DEBUG]: <%s:%s> INSERTING(%s)",
                   MOD, meth, tostring(value));

    -- apply the transform (if needed)
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
    return 1 -- show caller we did an insert
  end

  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
    MOD, meth, tostring(value));
  return nil;
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result. 
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- Parms:
-- (*) binList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanListAll(topRec, resultList, lsetCtrlMap) 
  local meth = "simpleScanListAll()";
  GP=F and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lsetCtrlMap.Transform ~= nil then
    transform = functionTable[lsetCtrlMap.Transform];
  end

  if lsetCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[lsetCtrlMap.UnTransform];
  end

  -- Loop through all the modulo n lset-record bins 
  local distrib = lsetCtrlMap.Modulo;

  GP=F and trace(" Number of Lset bins to parse: %d ", distrib)

  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
   
    GP=F and trace(" Parsing through :%s ", tostring(binName))

	if topRec[binName] ~= nil then
		local binList = topRec[binName];
		for i = 1, list.size( binList ), 1 do
			if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
				retValue = binList[i]; 
				if unTransform ~= nil then
					retValue = unTransform( binList[i] );
				end
		        list.append( resultList, retValue);
				listCount = listCount + 1; 
   			end -- end if not null and not empty
  		end -- end for each item in the list
    end -- end of topRec null check 
  end -- end for distrib list for-loop 

  GP=F and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)

  return 0; 
end -- simpleScanListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result.
--
-- TODO :  
-- This is COMPLEX SCAN, currently an exact copy of the simpleScanListAll().
-- I need to first write an unTransformComplexCompare() which involves
-- using the compare function, to write a new complexScanListAll()  
--
-- Parms:
-- (*) binList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanListAll(topRec, resultList, lsetCtrlMap) 
  local meth = "complexScanListAll()";
  GP=F and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lsetCtrlMap.Transform ~= nil then
    transform = functionTable[lsetCtrlMap.Transform];
  end

  if lsetCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[lsetCtrlMap.UnTransform];
  end

  -- Loop through all the modulo n lset-record bins 
  local distrib = lsetCtrlMap.Modulo;

  GP=F and trace(" Number of Lset bins to parse: %d ", distrib)

  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    GP=F and trace(" Parsing through :%s ", tostring(binName))
	local binList = topRec[binName];
	local resultValue = nil;
    if topRec[binName] ~= nil then
		for i = 1, list.size( binList ), 1 do
			if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
				retValue = binList[i]; 
				if unTransform ~= nil then
					retValue = unTransform( binList[i] );
				end
		  	    list.append( resultList, retValue);
				listCount = listCount + 1; 
   			end -- end if not null and not empty
  		end -- end for each item in the list
    end -- end of topRec null check 
  end -- end for distrib list for-loop 

 GP=F and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)

  return 0; 
end -- complexScanListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) binList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For FV_INSERT:
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanList(resultList, lsetCtrlMap, binList, value, flag ) 
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 MOD, meth, tostring(value), list.size(binList),
                 tostring(binList))

  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  if lsetCtrlMap.Transform ~= nil then
    transform = functionTable[lsetCtrlMap.Transform];
  end

  if lsetCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[lsetCtrlMap.UnTransform];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
      resultValue = unTransformSimpleCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          binList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = lsetCtrlMap.ItemCount;
          lsetCtrlMap.ItemCount = itemCount - 1;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result (only for scan and delete, not insert)
        list.append( resultList, resultValue );
        return 0; -- Found it. Return with success.
      end -- end if found it
    end -- end if not null and not empty
  end -- end for each item in the list

  -- Didn't find it.  If FV_INSERT, then append the value to the list
  -- Ideally, if we noticed a hole, we should use THAT for insert and not
  -- make the list longer.
  -- TODO: Fill in holes if we notice a lot of gas in the lists.
  if flag == FV_INSERT then
    GP=F and trace("[EXIT]: <%s:%s> Inserting(%s)",
                   MOD, meth, tostring(value));
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
    return 1 -- show caller we did an insert
  end
  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
                 MOD, meth, tostring(value));
  return 0; -- All is well.
end -- simpleScanList


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) lsetCtrlMap: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( resultList, lsetCtrlMap, binList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";

  GP=F and trace("[ENTER]:<%s:%s>Res(%s)Mp(%s)BL(%s)SV(%s)Fg(%s)F(%s)Frgs(%s)",
      MOD, meth, tostring( resultList), tostring(lsetCtrlMap),
      tostring(binList), tostring(searchValue), tostring(flag),
      tostring( filter ), tostring( fargs ));

  GP=F and trace("[DEBUG]:<%s:%s> KeyType(%s) A(%s) C(%s)",
      MOD, meth, tostring(lsetCtrlMap.KeyType), tostring(KT_ATOMIC),
      tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if lsetCtrlMap.KeyType == KT_ATOMIC then
    return simpleScanList(resultList, lsetCtrlMap, binList, searchValue, flag ) 
  else
    return complexScanList(resultList, lsetCtrlMap, binList, searchValue, flag ) 
  end
end


-- ======================================================================
-- localInsert( lsetCtrlMap, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) lsetCtrlMap: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, lsetCtrlMap, newValue, stats )
  local meth = "localInsert()";
  GP=F and trace("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));

  -- Notice that "computeSetBin()" will know which number to return, depending
  -- on whether we're in "compact" or "regular" storageState.
  local binNumber = computeSetBin( newValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  GP=F and trace("[DEBUG]:<%s:%s> Compute:BNum(%s) BName(%s) Val(%s) Map(%s)",
                 MOD, meth, tostring(binNumber), tostring(binName),
                 tostring(newValue), tostring(lsetCtrlMap));

  local binList = topRec[binName];

  local insertResult = 0;
  if binList == nil then
    GP=F and warn("[INTERNAL ERROR]:<%s:%s> binList is nil: binName(%s)",
                   MOD, meth, tostring( binName ) );
    error('Insert: INTERNAL ERROR: Nil Bin');
  else
    -- Look for the value, and insert if it is not there.
    insertResult =
      scanList( nil, lsetCtrlMap, binList, newValue, FV_INSERT, nil, nil );
    -- list.append( binList, newValue );
    topRec[binName] = binList;
  end

  GP=F and trace("[DEBUG]: <%s:%s>:Bin(%s) Now has list(%s)",
                 MOD, meth, binName, tostring(binList) );

  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = lsetCtrlMap.ItemCount;
    local totalCount = lsetCtrlMap.TotalCount;
    lsetCtrlMap.ItemCount = itemCount + 1; -- number of valid items goes up
    lsetCtrlMap.TotalCount = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", MOD, meth, itemCount );
  end
  topRec[LSET_CONTROL_BIN] = lsetCtrlMap;

  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 MOD, meth, tostring( newValue ), tostring( lsetCtrlMap ) );
    -- No need to return anything
end -- localInsert

-- ======================================================================
-- rehashSet( topRec, lsetBinName, lsetCtrlMap )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) lsetBinName
-- (*) lsetCtrlMap
-- ======================================================================
local function rehashSet( topRec, lsetBinName, lsetCtrlMap )
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  local singleBinName = getBinName( 0 );
  local singleBinList = topRec[singleBinName];
  if singleBinList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(singleBinName));
    error('BAD BIN 0 LIST for Rehash');
  end
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  lsetCtrlMap.StoreState = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = lsetCtrlMap.Modulo;
  for i = 0, (distrib - 1), 1 do
    setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localInsert( topRec, lsetCtrlMap, listCopy[i], 0 ); -- do NOT update counts.
  end

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- rehashSet()


-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- NOTE: Not needed while we're still hard-coding the LSET_CONTROL_BIN
-- name.
-- ======================================================================
local function validateBinName( binName )
  local meth = "validateBinName()";
  GP=F and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
    MOD, meth, tostring(binName));

  if binName == nil  then
    error('Bin Name Validation Error: Null BinName');
  elseif type( binName ) ~= "string"  then
    error('Bin Name Validation Error: BinName must be a string');
  elseif string.len( binName ) > 14 then
    error('Bin Name Validation Error: Exceeds 14 characters');
  end
end -- validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the lsetBinName and lsetMap are valid, otherwise
-- jump out with an error() call.
-- NOTE: In this version of Large Set, there is a SINGLE dedicated
-- Set bin and it is hard coded:  LSET_CONTROL_BIN => 'LSetControlBin'
-- So -- there's no point in checking that the CONTROL BIN name is ok,
-- since it MUST be (it's defined in this code).
-- Also -- we don't check the user's supplied bin name since that is not
-- used -- the bin name is hard-coded (for now).  We only care about the
-- existence of the Record, the control bin and the Map Integrity.
--
-- Parms:
-- (*) topRec:
-- (*) userBinName: User's Name -- not currently used
-- ======================================================================
local function validateRecBinAndMap( topRec, userBinName, mustExist )
  local meth = "validateRecBinAndMap()";

  GP=F and trace("[ENTER]: <%s:%s>  ", MOD, meth );

  -- Validate that the user's supplied BinName will work:
  -- ==========================================================
  -- NOTE: -- No need to check LSET Control bin name (for now) because
  -- we're not using the user's name -- only our own preset name.  Later,
  -- when we change to a single bin (named by the user), THEN we'll need
  -- to validate the user's name.
  -- validateBinName( binName );
  -- ==========================================================

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid Bin
  -- (*) Must have a valid Map in the bin.
  --
  -- If "mustExist" is false, then basically we're just going to check
  -- that our bin includes MAGIC, if it is non-nil.
  if mustExist == true then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error('Base Record Does NOT exist');
    end

    -- Control Bin Must Exist
    if( topRec[LSET_CONTROL_BIN] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSET_BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(LSET_CONTROL_BIN) );
      error('LSET_BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local lsetMap = topRec[LSET_CONTROL_BIN]; -- The main lset map
    if lsetMap.Magic ~= MAGIC or lsetMap.LdtType ~= LDT_LSET then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LSET_BIN(%s) Corrupted:No magic:1",
            MOD, meth, LSET_CONTROL_BIN );
      error('LSET_BIN Is Corrupted (No Magic)');
    end
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[LSET_CONTROL_BIN] ~= nil then
      local lsetMap = topRec[LSET_CONTROL_BIN];
      if lsetMap.Magic ~= MAGIC or lsetMap.LdtType ~= LDT_LSET then
        GP=F and warn("[ERROR EXIT]:<%s:%s>LSET_BIN<%s:%s>Corrupted:No magic:2",
              MOD, meth, LSET_CONTROL_BIN, tostring( lsetMap ));
        error('LSET_BIN Is Corrupted (No Magic::2)');
      end
    end
  end
end -- validateRecBinAndMap()


-- ======================================================================
-- validateTopRec( topRec, lsetMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LSET bin from the rec and check for magic
-- Return: True (good) or False (bad).
-- NOTE: >>>>   Currently not used.  <<<<<
-- ======================================================================
local function  validateTopRec( topRec, lsetMap )
  local thisMap = topRec[ LSET_CONTROL_BIN ];
  if thisMap.Magic == MAGIC then
    return "good"
  else
    return "bad"
  end
end -- validateTopRec()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Requirements/Restrictions (this version).
-- (1) One Set Per Record
--
-- ======================================================================
-- || lset_create ||
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
-- (*) lsetBinName: The name of the bin for the AS Large Set
-- (*) createSpec: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
--
function lset_create( topRec, lsetBinName, createSpec )
  local meth = "lset_create()";
  GP=F and trace("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(lsetBinName), tostring(createSpec) );

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LSET field to an
  -- existing record.
  if( topRec[LSET_CONTROL_BIN] ~= nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> LSET CONTROL BIN Already Exists",
                   MOD, meth );
    error('LSET CONTROL BIN already exists');
  end
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.
  
  -- This will throw and error and jump out of Lua if binName is bad.
  -- NOTE: Not needed until we switch to using a SINGLE BIN for LSET.
  -- validateBinName( lsetBinName );

  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
  local lsetCtrlMap;
  lsetCtrlMap = initializeLSetMap( topRec, lsetBinName );
  
  -- If the user has passed in some settings that override our defaults
  -- (createSpec) then apply them now.
  if createSpec ~= nil then 
    adjustLSetMap( lsetCtrlMap, createSpec ); end

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
                 MOD, meth , tostring(lsetCtrlMap));

  topRec[LSET_CONTROL_BIN] = lsetCtrlMap; -- store in the record

  -- initializeLSetMap always sets lsetCtrlMap.StoreState to SS_COMPACT
  -- At this point there is only one bin.
  setupNewBin( topRec, 0 );

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- lset_create()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || AS Large Set Insert (with and without Create)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Insert a value into the set.
-- Take the value, perform a hash and a modulo function to determine which
-- bin list is used, then add to the list.
--
-- We will use predetermined BIN names for this initial prototype
-- 'LSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of data
-- Notice that this means that THERE CAN BE ONLY ONE AS Set object per record.
-- In the final version, this will change -- there will be multiple 
-- AS Set bins per record.  We will switch to a modified bin naming scheme.
--
-- NOTE: Design, V2.  We will cache all data in the FIRST BIN until we
-- reach a certain number N (e.g. 100), and then at N+1 we will create
-- all of the remaining bins in the record and redistribute the numbers, 
-- then insert the 101th value.  That way we save the initial storage
-- cost of small, inactive or dead users.
-- ==> The CtrlMap will show which state we are in:
-- (*) StoreState=SS_COMPACT: We are in SINGLE BIN state (no hash)
-- (*) StoreState=SS_REGULAR: We hash, mod N, then insert (append) into THAT bin.
--
-- +========================================================================+=~
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... | ~
-- +========================================================================+=~
--    ~=+===========================================+
--    ~ | Set Bin 1 | Set Bin 2 | o o o | Set Bin N |
--    ~=+===========================================+
--            V           V                   V
--        +=======+   +=======+           +=======+
--        |V List |   |V List |           |V List |
--        +=======+   +=======+           +=======+
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) lsetBinName: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- (*) createSpec: When in "Create Mode", use this Create Spec
local function localLSetInsert( topRec, lsetBinName, newValue, createSpec )
  local meth = "localLSetInsert()";
  GP=F and trace("[ENTER]:<%s:%s> SetBin(%s) NewValue(%s) createSpec(%s)",
                 MOD, meth, tostring(lsetBinName), tostring( newValue ),
                 tostring( createSpec ));

  local lsetCtrlMap;
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, false );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[LSET_CONTROL_BIN] == nil ) then
    warn("[WARNING]: <%s:%s> LSET CONTROL BIN does not Exist:Creating",
         MOD, meth );
    lsetCtrlMap =
      initializeLSetMap( topRec, lsetBinName );
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLSetMap( lsetCtrlMap, createSpec );
    end

    topRec[LSET_CONTROL_BIN] = lsetCtrlMap;
    -- initializeLSetMap always sets lsetCtrlMap.StoreState to SS_COMPACT
    -- At this point there is only one bin
    setupNewBin( topRec, 0 ); -- set up Bin ZERO
  else
    lsetCtrlMap = topRec[LSET_CONTROL_BIN];
  end

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local totalCount = lsetCtrlMap.TotalCount;
  if lsetCtrlMap.StoreState == SS_COMPACT and
      totalCount >= lsetCtrlMap.ThreshHold
  then
    rehashSet( topRec, lsetBinName, lsetCtrlMap );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, lsetCtrlMap, newValue, 1 );

  topRec[LSET_CONTROL_BIN] = lsetCtrlMap;
  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc
end -- function localLSetInsert()


-- ======================================================================
-- lset_insert() -- with and without create
-- ======================================================================
function lset_insert( topRec, lsetBinName, newValue )
  return localLSetInsert( topRec, lsetBinName, newValue, nil )
end -- lset_insert()

function lset_create_and_insert( topRec, lsetBinName, newValue, createSpec )
  return localLSetInsert( topRec, lsetBinName, newValue, createSpec )
end -- lset_create_and_insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Large Set Exists
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return 1 item if the item exists in the set, otherwise return 0.
-- We don't want to return "true" and "false" because of Lua Weirdness.
-- Parms:
--
-- Return:
-- ======================================================================
local function localLSetExists(topRec,lsetBinName,searchValue,filter,fargs )
  local meth = "localLSetExists()";
  GP=F and trace("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec[LSET_CONTROL_BIN];
  local binNumber = computeSetBin( searchValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local resultList = list();
  local result = scanList( resultList, lsetCtrlMap, binList, searchValue,
                            FV_SCAN, filter, fargs);
  if result == nil then
    return 0
  else
    return 1
  end
end -- function localLSetExists()

-- ======================================================================
-- lset_exists() -- with and without filter
-- ======================================================================
function lset_exists( topRec, lsetBinName, searchValue )
  return localLSetExists( topRec, lsetBinName, searchValue, nil, nil )
end -- lset_exists()

function lset_exists_then_filter( topRec, lsetBinName, searchValue,
                                  filter, fargs )
  return localLSetExists( topRec, lsetBinName, searchValue, filter, fargs );
end -- lset_exists_then_filter()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return the item if the item exists in the set.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
local function localLSetSearch(resultList, topRec, lsetBinName, searchValue,
                filter, fargs)

  local meth = "localLSetSearch()";
  rc = 0; -- start out OK.
  GP=F and trace("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );

  -- Define our return list.
  local resultList = list();

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec[LSET_CONTROL_BIN];
  local binNumber = computeSetBin( searchValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  rc = 
    scanList(resultList,lsetCtrlMap,binList,searchValue,FV_SCAN,filter,fargs);

  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
                 MOD, meth, tostring(result));

  return resultList;
end -- function localLSetSearch()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search All
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- Version of lset-search when search-value is null
-- Return all the list-items from the lset-bin as a result-list 
-- This is basically a nested for-loop version of localLSetSearch() 
--
-- ======================================================================
local function localLSetSearchAll(resultList, topRec, lsetBinName,
                filter, fargs)

  local meth = "localLSetSearchAll()";
  rc = 0; -- start out OK.
  GP=F and trace(" <%s:%s> Null search-value, return all elements. Name: %s, top-rec %s ",
                 MOD, meth, tostring(lsetBinName), tostring(topRec));

  -- Define our return list.
  local resultList = list();
  local distrib_list = list();

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec[LSET_CONTROL_BIN];
 
  if lsetCtrlMap.KeyType == KT_ATOMIC then
	rc = simpleScanListAll(topRec, resultList, lsetCtrlMap) 
  else
	rc = complexScanListAll(topRec, resultList, lsetCtrlMap)
  end

  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s) Size : %d",
                 MOD, meth, tostring(resultList), list.size(resultList));

  return resultList; 
end -- function localLSetSearchAll()

-- ======================================================================
-- lset_search() -- with and without filter
-- ======================================================================
function lset_search( topRec, lsetBinName, searchValue )
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue
  if( searchValue == nil ) then
	  return localLSetSearchAll(resultList,topRec,lsetBinName,nil,nil)
  else
	  return localLSetSearch(resultList,topRec,lsetBinName,searchValue,nil,nil)
  end
end -- lset_search()

function lset_search_then_filter( topRec, lsetBinName, searchValue,
                                  filter, fargs )
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue
  if( searchValue == nil ) then
	return localLSetSearchAll(resultList,topRec,lsetBinName,filter,fargs)
  else
  	return localLSetSearch(resultList,topRec,lsetBinName,searchValue,filter,fargs)
  end
end -- lset_search_then_filter()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- ======================================================================
local function localLSetDelete( topRec, lsetBinName, deleteValue,
        filter, fargs)

  local meth = "lset_delete()";
  GP=F and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.
  local resultList = list(); -- add results to this list.

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[LSET_CONTROL_BIN] == nil ) then
    GP=F and trace("[ERROR EXIT]: <%s:%s> LSetCtrlBin does not Exist",
                   MOD, meth );
    error('LSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec[LSET_CONTROL_BIN];
  local binNumber = computeSetBin( deleteValue, lsetCtrlMap );

  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  -- Fow now, scanList() will only NULL out the element in a list, but will
  -- not collapse it.  Later, if we see that there are a LOT of nil entries,
  -- we can RESET the set and remove all of the "gas".
  rc = scanList(resultList,lsetCtrlMap,binList,deleteValue,FV_DELETE,nil,nil);
  -- If we found something, then we need to update the bin and the record.
  if rc == 0 and list.size( resultList ) > 0 then
    -- We found something -- and marked it nil -- so update the record
    topRec[binName] = binList;
    rc = aerospike:update( topRec );
    if( rc < 0 ) then
      error('Delete Error on Update Record');
    end
  else
    resultList = list();
  end

  GP=F and trace("[EXIT]: <%s:%s>: Delete RC(%d) ResultList(%s)",
                 MOD, meth, rc, tostring( resultList ));

  return resultList;
end -- function lset_delete()

-- ======================================================================
-- lset_delete() -- with and without filter
-- Return resultList
-- (*) If successful: return deleted items (list.size( resultList ) > 0)
-- (*) If error: resultList will be an empty list.
-- ======================================================================
function lset_delete( topRec, lsetBinName, searchValue )

  return localLSetDelete(topRec, lsetBinName, searchValue, nil, nil )
end -- lset_delete()

function lset_delete_then_filter( topRec, lsetBinName, searchValue,
                                  filter, fargs )

  return localLSetDelete( topRec, lsetBinName, searchValue,
                          filter, fargs )
end -- lset_delete_then_filter()

-- ========================================================================
-- lset_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function lset_size( topRec, lsetBinName )
  local meth = "lset_size()";

  GP=F and trace("[ENTER1]: <%s:%s> lsetBinName(%s)",
  MOD, meth, tostring(lsetBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  local lsetMap = topRec[ LSET_CONTROL_BIN ];
  local itemCount = lsetMap.ItemCount;

  GP=F and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function lset_size()

-- ========================================================================
-- lset_config() -- return the config settings
-- ========================================================================
function lset_config( topRec, lsetBinName )
  local meth = "lset_config()";

  GP=F and trace("[ENTER1]: <%s:%s> lsetBinName(%s)",
  MOD, meth, tostring(lsetBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  local config = lsetSummary( topRec[ LSET_CONTROL_BIN ] );

  GP=F and trace("[EXIT]:<%s:%s>:config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lset_config()

-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
