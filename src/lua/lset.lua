-- AS Large Set (LSET) Operations
-- Last Update July 31, 2013: TJL
--
-- Keep this in sync with the version above.
local MOD="lset_2013_07_31.i"; -- the module name used for tracing

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.1;

-- Please refer to lset_design.lua for architecture and design notes.
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
-- set up our "outside" links.
-- We use this to get our Hash Functions
local  CRC32 = require('CRC32');
-- We use this to get access to all of the Functions
local functionTable = require('UdfFunctionTable');
-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt_errors');

-- This flavor of LDT
local LDT_LSET   = "LSET";

-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

-- NOTE: When we finally fix LIST handling in Lua, we'll be able to NULL
-- out a cell by assigning "nil" -- and we'll stop using this goofy trick.
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

-- HashType (HT) values
local HT_STATIC  ='S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC ='D'; -- Use a DYNAMIC set of bins for hash lists

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
-- Test Modes
local PackageTestModeObject  = "TestModeObject";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
-- Debug Modes
local PackageDebugModeObject = "DebugModeObject";
local PackageDebugModeList   = "DebugModeList";
local PackageDebugModeBinary = "DebugModeBinary";
local PackageDebugModeNumber = "DebugModeNumber";

-- Enhancements for LSET begin here 

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
-- come back to bite me.
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (CDIR NOT used in this context)
-- (2) As a TYPE in our own propMap[PM_RecType] field: CDIR *IS* used here.
local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
local RT_CDIR= 3; -- xxx: Cold Dir Subrec::Not used for set_type() 
local RT_ESR = 4; -- 0x4: Existence Sub Record

-- LDT TYPES (only lstack is defined here)
local LDT_TYPE_LSET = "LSET";

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

---- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values. (There's no secret message hidden in these values).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common across lset, lstack & lmap 
local RPM_LdtCount             = 'C';  -- Number of LDTs in this rec
local RPM_VInfo                = 'V';  -- Partition Version Info
local RPM_Magic                = 'Z';  -- Special Sauce
local RPM_SelfDigest           = 'D';  -- Digest of this record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common for all LDT's
local PM_ItemCount             = 'I'; -- (Top): Count of all items in LDT
local PM_Version               = 'V'; -- (Top): Code Version
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_EsrDigest             = 'E'; -- (All): Digest of ESR
local PM_RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
local PM_LogInfo               = 'L'; -- (All): Log Info (currently unused)
local PM_ParentDigest          = 'P'; -- (Subrec): Digest of TopRec
local PM_SelfDigest            = 'D'; -- (Subrec): Digest of THIS Record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lset & lmap 
local M_StoreMode              = 'M'; -- SM_LIST or SM_BINARY
local M_StoreLimit             = 'L'; -- Used for Eviction (eventually)
local M_Transform              = 't'; -- Transform object to Binary form
local M_UnTransform            = 'u'; -- UnTransform object from Binary form
local M_KeyCompare             = 'k'; -- User Supplied Key Compare Function
local M_StoreState             = 'S'; -- Store State (Compect or List)
local M_HashType               = 'h'; -- Hash Type (static or dynamic)
local M_BinaryStoreSize        = 'B'; -- Size of Object when in Binary form
local M_KeyType                = 'K'; -- Key Type: Atomic or Complex
local M_TotalCount             = 'C'; -- Total number of slots used
local M_Modulo 				   = 'm'; -- Modulo used for Hash Function
local M_ThreshHold             = 'H'; -- Threshold: Compact->Regular state
local M_KeyFunction            = 'F'; -- User Supplied Key Extract Function
-- ------------------------------------------------------------------------
-- Maintain the LSET letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
--
-- A:                         a:                         0:
-- B:M_BinaryStoreSize        b:                         1:
-- C:M_TotalCount             c:                         2:
-- D:                         d:                         3:
-- E:                         e:                         4:
-- F:M_KeyFunction            f:                         5:
-- G:                         g:                         6:
-- H:M_Threshold              h:                         7:
-- I:                         i:                         8:
-- J:                         j:                         9:
-- K:M_KeyType                k:M_KeyCompare     
-- L:                         l:                       
-- M:M_StoreMode              m:M_Modulo
-- N:                         n:
-- O:                         o:
-- P:                         p:
-- Q:                         q:
-- R:                         r:                     
-- S:M_StoreLimit             s:                     
-- T:                         t:M_Transform
-- U:                         u:M_UnTransform
-- V:                         v:
-- W:                         w:                     
-- X:                         x:                     
-- Y:                         y:
-- Z:                         z:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--
-- ++====================++
-- || INTERNAL BIN NAMES || -- Local, but global to this module
-- ++====================++
-- The Top Rec LDT bin is named by the user -- so there's no hardcoded name
-- for each used LDT bin.
--
-- In the main record, there is one special hardcoded bin -- that holds
-- some shared information for all LDTs.
-- Note the 14 character limit on Aerospike Bin Names.
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local REC_LDT_CTRL_BIN  = "LDTCONTROLBIN"; -- Single bin for all LDT in rec

-- There are THREE different types of (Child) subrecords that are associated
-- with an LSTACK LDT:
-- (1) LDR (Lso Data Record) -- used in both the Warm and Cold Lists
-- (2) ColdDir Record -- used to hold lists of LDRs (the Cold List Dirs)
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT subrecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN   = "SR_PROP_BIN";
--
-- The Lso Data Records (LDRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_LIST_BIN      = "LdrListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- Enhancements for LSET end here 

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
local function packageStandardList( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_ATOMIC; -- Atomic Keys
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo]= DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many inserts
 
end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_ATOMIC; -- Atomic Keys
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many inserts
 
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
local function packageTestModeObject( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  lsetMap[M_KeyFunction] = "keyExtract"; -- Defined in UdfFunctionTable
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many inserts
 
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many inserts
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = "compressTest4";
  lsetMap[M_UnTransform] = "unCompressTest4";
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many inserts

end -- packageTestModeBinary( lsetMap )

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSET with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
local function packageDebugModeObject( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if not using it
  lsetMap[M_KeyType] = KT_COMPLEX; -- Complex Key (must be extracted)
  lsetMap[M_KeyFunction] = "keyExtract"; -- Defined in UdfFunctionTable
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 4; -- Rehash after this many inserts

end -- packageDebugModeObject()


-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSET with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = nil; -- Don't waste room if not using it
  lsetMap[M_KeyType] = KT_ATOMIC; -- Atomic Keys
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 4; -- Rehash after this many inserts

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = "compressTest4";
  lsetMap[M_UnTransform] = "unCompressTest4";
  lsetMap[M_KeyCompare] = "debugListCompareEqual"; -- "Simple" list comp
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = 16; -- Set to the exact fixed size
  lsetMap[M_KeyType] = KT_COMPLEX; -- special function for list compare.
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 4; -- Rehash after this many inserts

end -- packageDebugModeBinary( lsetMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( lsetMap )
  local meth = "packageDebugModeNumber()";
  GP=F and trace("[ENTER]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring( lsetMap ));
  
  -- General Parameters
  lsetMap[M_Transform] = nil;
  lsetMap[M_UnTransform] = nil;
  lsetMap[M_KeyCompare] = nil;
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode ]= SM_LIST; -- Use List Mode
  lsetMap[M_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
  lsetMap[M_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 4; -- Rehash after this many inserts

  GP=F and trace("[EXIT]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring( lsetMap ));
end -- packageDebugModeNumber( lsetMap )

-- ======================================================================
-- Package = "StumbleUpon"
-- StumbleUpon uses a compacted representation.
-- NOTE: This will eventually move to the UDF Function Table, or to a
-- separate Configuration file.  For the moment it is included here for
-- convenience. 
-- ======================================================================
local function packageStumbleUpon( lsetMap )
  
  -- General Parameters
  lsetMap[M_Transform] = "compress4ByteInteger";
  lsetMap[M_UnTransform] = "unCompress4ByteInteger";
  lsetMap[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lsetMap[M_StoreMode] = SM_BINARY; -- Use a Byte Array
  lsetMap[M_BinaryStoreSize] = 4; -- Storing a single 4 byte integer
  lsetMap[M_KeyType] = KT_ATOMIC; -- Atomic Keys (a number)
  lsetMap[M_BinName] = LSET_CONTROL_BIN;
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 100; -- Rehash after this many inserts
  
end -- packageStumbleUpon( lsetMap )

-- ======================================================================
-- adjustLSetMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the lsetMap.
-- Parms:
-- (*) lsetMap: the main LSET Bin value
-- (*) argListMap: Map of LSET Settings 
-- ======================================================================
local function adjustLSetMap( lsetMap, argListMap )
  local meth = "adjustLSetMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsetMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(lsetMap), tostring( argListMap ));

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
          packageStandardList( lsetMap );
      -- Test Mode Cases
      elseif value == PackageTestModeObject then
          packageTestModeObject( lsetMap );
      elseif value == PackageTestModeList then
          packageTestModeList( lsetMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( lsetMap );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( lsetMap );
      -- DEBUG Mode Cases
      elseif value == PackageDebugModeObject then
          packageDebugModeObject( lsetMap );
      elseif value == PackageDebugModeList then
          packageDebugModeList( lsetMap );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( lsetMap );
      elseif value == PackageDebugModeNumber then
          packageDebugModeNumber( lsetMap );
      -- SPECIAL Cases (e.g. Stumble, etc)
      elseif value == PackageStumbleUpon then
          packageStumbleUpon( lsetMap );
      end
    elseif name == "KeyType" and type( value ) == "string" then
      -- Use only valid values (default to ATOMIC if not specifically complex)
      -- Allow both upper and lower case versions of "complex".
      if value == KT_COMPLEX or value == "complex" then
        lsetMap[M_KeyType] = KT_COMPLEX;
      else
        lsetMap[M_KeyType] = KT_ATOMIC; -- this is the default.
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        lsetMap[M_StoreMode] = value;
      end
    elseif name == "Modulo"  and type( value ) == "number" then
      -- Verify it's a valid value
      if value > 0 and value < MODULO_MAX then
        lsetMap[M_Modulo] = value;
      end
    end
  end -- for each argument

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(lsetMap));
      
  return lsetMap
end -- adjustLSetMap

-- ======================================================================
-- local function lsetSummary( lsetList ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsetMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsetSummary( lsetList )

  if ( lsetList == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    return "EMPTY LDT BIN VALUE";
  end

  local propMap = lsetList[1];
  local lsetMap  = lsetList[2];
  
  if( propMap[PM_Magic] ~= MAGIC ) then
    return "BROKEN MAP--No Magic";
  end;

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  resultMap.SUMMARY              = "LSET Summary";
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  
    -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = lsetMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = lsetMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = lsetMap[M_LdrByteCountMax];
  
  -- General LSO Parms:
  resultMap.StoreMode            = lsetMap[M_StoreMode];
  resultMap.StoreState           = lsetMap[M_StoreState];
  resultMap.StoreLimit           = lsetMap[M_StoreLimit];
  resultMap.Transform            = lsetMap[M_Transform];
  resultMap.UnTransform          = lsetMap[M_UnTransform];
  resultMap.KeyCompare           = lsetMap[M_KeyCompare];
  resultMap.BinaryStoreSize      = lsetMap[M_BinaryStoreSize];
  resultMap.KeyType              = lsetMap[M_KeyType];
  resultMap.TotalCount			 = lsetMap[M_TotalCount];		
  resultMap.Modulo 				 = lsetMap[M_Modulo];
  resultMap.ThreshHold			 = lsetMap[M_ThreshHold];

  return resultMap;
end -- lsetSummary()

-- ======================================================================
-- local function lsetSummaryString( lsetList ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsetMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsetSummaryString( lsetList )
   GP=F and info("Calling lsetSummaryString "); 
  return tostring( lsetSummary( lsetList ));
end -- lsetSummaryString()

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
-- Return: The initialized lsetMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLSetMap(topRec, lsetBinName )
  local meth = "initializeLSetMap()";
  GP=F and trace("[ENTER]: <%s:%s>::Bin(%s)",MOD, meth, tostring(lsetBinName));
  
  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific Lso Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local lsetMap = map();
  local lsetList = list();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount] = 0; -- A count of all items in the stack
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LSET; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = lsetBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]    = nil; -- not set yet.

  -- Specific LSET Parms: Held in lsetMap
  lsetMap[M_StoreMode]   = SM_LIST; -- SM_LIST or SM_BINARY:
  lsetMap[M_StoreLimit]  = 0; -- No storage Limit

  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsetMap[M_LdrEntryCountMax]= 100;  -- Max # of Data Chunk items (List Mode)
  lsetMap[M_LdrByteEntrySize]=  0;  -- Byte size of a fixed size Byte Entry
  lsetMap[M_LdrByteCountMax] =   0; -- Max # of Data Chunk Bytes (binary mode)

  lsetMap[M_Transform]        = nil; -- applies only to complex lmap
  lsetMap[M_UnTransform]      = nil; -- applies only to complex lmap
  lsetMap[M_KeyCompare]       = nil; -- applies only to complex lmap
  lsetMap[M_StoreState]       = SS_COMPACT; -- SM_LIST or SM_BINARY:
  lsetMap[M_HashType]         = HT_STATIC; -- Static or Dynamic
  lsetMap[M_BinaryStoreSize]  = nil; 
  lsetMap[M_KeyType]          = KT_ATOMIC; -- assume "atomic" values for now.
  lsetMap[M_TotalCount] = 0; -- Count of both valid and deleted elements
  lsetMap[M_Modulo] = DEFAULT_DISTRIB;
  lsetMap[M_ThreshHold] = 101; -- Rehash after this many inserts

  -- Put our new maps in a list, in the record, then store the record.
  list.append( lsetList, propMap );
  list.append( lsetList, lsetMap );
  topRec[LSET_CONTROL_BIN]            = lsetList;

  GP=F and info("[DEBUG]: <%s:%s> : LSET Summary after Init(%s)",
      MOD, meth , lsetSummaryString(lsetList));


  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return lsetList;

end -- initializeLSetMap()

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
  -- create the first LsetBin_n LDT bin
  topRec[binName] = list(); -- Create a new list for this new bin

  GP=F and trace("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)",
                 MOD, meth, binNum, binName );

  return binName;
end -- setupNewBin

-- ======================================================================
-- The value is either simple (atomic) or an object (complex).  Complex
-- objects either have a key function defined, or they have a field called
-- "key" that will give us a key value.
-- If none of these are true -- then return -1 to show our displeasure.
-- ======================================================================
local function getKeyValue( ldtMap, value )
  local meth = "getKeyValue()";
  GP=F and trace("[ENTER]<%s:%s> value(%s) KeyType(%s)",
    MOD, meth, tostring(value), tostring(ldtMap[M_KeyType]) );

  local keyValue;
  if( ldtMap[M_KeyType] == KT_ATOMIC ) then
    keyValue = value;
  else
    -- Employ the user's supplied function (keyFunction) and if that's not
    -- there, look for the special case where the object has a field
    -- called 'key'.  If not, then, well ... tough.  We tried.
    local keyFunction = ldtMap[M_KeyFunction];
    if( keyFunction ~= nil ) and functionTable[keyFunction] ~= nil then
      keyValue = functionTable[keyFunction]( value );
    elseif value["key"] ~= nil then
      keyValue = value["key"];
    else
      keyValue = -1;
    end
  end

  GP=F and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
  return keyValue;
end -- getKeyValue();

-- ======================================================================
-- computeSetBin()
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single bin.
-- Second -- use the right hash function (depending on the type).
-- And, know if it's an atomic type or complex type.
-- ======================================================================
local function computeSetBin( newValue, lsetMap )
  local meth = "computeSetBin()";
  GP=F and trace("[ENTER]: <%s:%s> val(%s) Map(%s) ",
                 MOD, meth, tostring(newValue), tostring(lsetMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local binNumber  = 0;
  local key = newValue; -- assume atomic, change if complex.
  if lsetMap[M_StoreState] == SS_COMPACT then
    return 0
  else
    if( lsetMap[M_KeyType] == KT_COMPLEX ) then
      key = getKeyValue( lsetMap, newValue );
    end

    -- There are really only TWO primitive types that we can handle,
    -- and that is NUMBER and STRING.  Anything else is just wrong!!
    if type(key) == "number" then
      binNumber  = numberHash( key, lsetMap[M_Modulo] );
    elseif type(key) == "string" then
      binNumber  = stringHash( key, lsetMap[M_Modulo] );
    else
      warn("[INTERNAL ERROR]<%s:%s>Hash(%s) requires type number or string!",
        MOD, meth, type(key) );
      error( ldte.ERR_INTERNAL );
    end
  end
  GP=F and trace("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) ",
                 MOD, meth, tostring(newValue), binNumber );

  return binNumber;
end -- computeSetBin()

-- ======================================================================
-- listAppend()
-- ======================================================================
-- General tool to append one list to another.   At the point that we
-- find a better/cheaper way to do this, then we change THIS method and
-- all of the LDT calls to handle lists will get better as well.
-- ======================================================================
local function listAppend( baseList, additionalList )
  if( baseList == nil ) then
    warn("[INTERNAL ERROR] Null baselist in listAppend()" );
    error( ldte.ERR_INTERNAL );
  end
  local listSize = list.size( additionalList );
  for i = 1, listSize, 1 do
    list.append( baseList, additionalList[i] );
  end -- for each element of additionalList

  return baseList;
end -- listAppend()
--

-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the lsetMap, if present, and apply
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
-- Take the UnTransform defined in the lsetMap, if present, and apply
-- it to the dbValue, returning the unTransformed value.  If no unTransform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyUnTransform( lsetMap, storeValue )
  local returnValue = storeValue;
  if lsetMap[M_UnTransform] ~= nil and
    functionTable[lsetMap[M_UnTransform]] ~= nil then
    returnValue = functionTable[lsetMap[M_UnTransform]]( storeValue );
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
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) resultList: List holding search result
-- (*) lsetList: Main LDT Control Structure
-- (*) binList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    Answer is attached to "resultList", Status is returned via function.
--    ERR_OK (0) if FOUND 
--    ERR_NOT_FOUND (-2) if NOT FOUND
-- For FV_INSERT:
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanList(resultList, lsetList, binList, value, flag ) 
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
     MOD, meth, tostring(value), list.size(binList), tostring(binList))
                 
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  if lsetMap[M_Transform] ~= nil then
    transform = functionTable[lsetMap[M_Transform]];
  end

  if lsetMap[M_UnTransform] ~= nil then
    unTransform = functionTable[lsetMap[M_UnTransform]];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
--  if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
    if binList[i] ~= nil then
      resultValue = unTransformSimpleCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
--        binList[i] = FV_EMPTY; -- the value is NO MORE
          binList[i] = nil; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lsetList[1] = propMap; 
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
  return ERR_NOT_FOUND; -- All is well, but NOT FOUND.
end -- simpleScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is COMPLEX SCAN, which means we are comparing the KEY field of the
-- map object in both the value and in the List.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) resultList: List holding search result
-- (*) lsetList: The main LDT control structure
-- (*) objList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    Answer is attached to "resultList", Status is returned via function.
--    ERR_OK (0) if FOUND 
--    ERR_NOT_FOUND (-2) if NOT FOUND
-- For insert (FV_INSERT):
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanList(resultList, lsetList, objList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];

  local transform = nil;
  local unTransform = nil;

  if lsetMap[M_Transform]~= nil then
    transform = functionTable[lsetMap[M_Transform]];
  end

  if lsetMap[M_UnTransform] ~= nil then
    unTransform = functionTable[lsetMap[M_UnTransform]];
  end

  local key = getKeyValue( lsetMap, value );

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( objList ), 1 do
    GP=F and trace("[DEBUG]<%s:%s> It(%d) Comparing KEY(%s) with DataVal(%s)",
                   MOD, meth, i, tostring(key), tostring(objList[i]));
--  if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
    if objList[i] ~= nil then
      resultValue = unTransformComplexCompare(unTransform, objList[i], key);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
--        objList[i] = FV_EMPTY; -- the value is NO MORE
          objList[i] = nil; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lsetList[1] = propMap;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result (only for scan and delete, not insert)
        list.append( resultList, resultValue );
        return 0;
      end -- end if found it
    end -- end if value not nil or empty
  end -- for each list entry in this objList

  -- Didn't find it.  If FV_INSERT, then append the value to the list
  if flag == FV_INSERT then
    GP=F and trace("[DEBUG]: <%s:%s> INSERTING(%s)",
                   MOD, meth, tostring(value));

    -- apply the transform (if needed)
    local storeValue = applyTransform( transform, value );
    list.append( objList, storeValue );
    return 1 -- show caller we did an insert
  end

  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
    MOD, meth, tostring(value));
  return ERR_NOT_FOUND; -- All is well, but NOT FOUND.
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result. 
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- Parms:
-- (*) objList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanListAll(topRec, resultList, lsetList) 
  local meth = "simpleScanListAll()";
  GP=F and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lsetMap[M_Transform] ~= nil then
    transform = functionTable[lsetMap[M_Transform]];
  end

  if lsetMap[M_UnTransform] ~= nil then
    unTransform = functionTable[lsetMap[M_UnTransform]];
  end

  -- Loop through all the modulo n lset-record bins 
  local distrib = lsetMap[M_Modulo];

  GP=F and trace(" Number of Lset bins to parse: %d ", distrib)

  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
   
    GP=F and trace(" Parsing through :%s ", tostring(binName))

	if topRec[binName] ~= nil then
		local objList = topRec[binName];
		for i = 1, list.size( objList ), 1 do
--          if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
			if objList[i] ~= nil then
				retValue = objList[i]; 
				if unTransform ~= nil then
					retValue = unTransform( objList[i] );
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
local function complexScanListAll(topRec, resultList, lsetList) 
  local meth = "complexScanListAll()";
  GP=F and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;
  
  if lsetMap[M_Transform] ~= nil then
    transform = functionTable[lsetMap[M_Transform]];
  end

  if lsetMap[M_UnTransform] ~= nil then
    unTransform = functionTable[lsetMap[M_UnTransform]];
  end

  -- Loop through all the modulo n lset-record bins 
  local distrib = lsetMap[M_Modulo];
  
  GP=F and trace(" Number of Lset bins to parse: %d ", distrib)

  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    GP=F and trace(" Parsing through :%s ", tostring(binName))
	local binList = topRec[binName];
	local resultValue = nil;
    if topRec[binName] ~= nil then
		for i = 1, list.size( binList ), 1 do
--			if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
			if binList[i] ~= nil then
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
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) resultList is nil when called for insertion 
-- (*) lsetList: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( resultList, lsetList, binList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";
  
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  
  GP=F and trace("[DEBUG]:<%s:%s> KeyType(%s) Atomic=(%s) Complex=(%s)",
              MOD, meth, tostring(lsetMap[M_KeyType]), tostring(KT_ATOMIC),
              tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if lsetMap[M_KeyType] == KT_ATOMIC then
    return simpleScanList(resultList, lsetList, binList, searchValue, flag ) 
  else
    return complexScanList(resultList, lsetList, binList, searchValue, flag ) 
  end
end


-- ======================================================================
-- localInsert()
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) lsetList: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, lsetList, newValue, stats )
  local meth = "localInsert()";
  
  GP=F and info("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));

  local propMap = lsetList[1];  
  local lsetMap = lsetList[2];
  
  -- Notice that "computeSetBin()" will know which number to return, depending
  -- on whether we're in "compact" or "regular" storageState.
  local binNumber = computeSetBin( newValue, lsetMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local insertResult = 0;
  
  if binList == nil then
    GP=F and warn("[INTERNAL ERROR]:<%s:%s> binList is nil: binName(%s)",
                 MOD, meth, tostring( binName ) );
    error( ldte.ERR_INSERT );
  else
    GP=F and trace("[INTERNAL DUMP]:<%s:%s> binList is NOT nil: binName(%s)",
                 MOD, meth, tostring( binName ) );
    -- Look for the value, and insert if it is not there.
    insertResult =
      scanList( nil, lsetList, binList, newValue, FV_INSERT, nil, nil );
    topRec[binName] = binList; 
  end
                
  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = lsetMap[M_TotalCount];
    
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    lsetMap[M_TotalCount] = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[STATUS]<%s:%s>Updating Stats TC(%d) IC(%d)", MOD, meth,
      lsetMap[M_TotalCount], propMap[PM_ItemCount] );
  else
    GP=F and trace("[STATUS]<%s:%s>NOT updating stats (%d)", MOD, meth, stats);
  end

  topRec[LSET_CONTROL_BIN] = lsetList;
 
  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 MOD, meth, tostring( newValue ), tostring( lsetMap ) );
  -- No need to return anything
end -- localInsert

-- ======================================================================
-- rehashSet( topRec, lsetBinName, lsetList )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) lsetBinName
-- (*) lsetList
-- ======================================================================
local function rehashSet( topRec, lsetBinName, lsetList )
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );

  local propMap = lsetList[1];  
  local lsetMap = lsetList[2];

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  local singleBinName = getBinName( 0 );
  local singleBinList = topRec[singleBinName];
  if singleBinList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(singleBinName));
    error( ldte.ERR_INSERT );
  end
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  lsetMap[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = lsetMap[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- assign topRec[binName]  to LDR list
    setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localInsert( topRec, lsetList, listCopy[i], 0 ); -- do NOT update counts.
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
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( binName ) ~= "string"  then
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( binName ) > 14 then
    error( ldte.ERR_BIN_NAME_TOO_LONG );
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
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
      
    -- Control Bin Must Exist, in this case, lsetList is what we check
    if( topRec[LSET_CONTROL_BIN] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSET_BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(LSET_CONTROL_BIN) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    local lsetList = topRec[LSET_CONTROL_BIN]; -- The main lset map
    local propMap = lsetList[1];
    local lsetMap  = lsetList[2];
    
    if(propMap[PM_Magic] ~= MAGIC) or propMap[PM_LdtType] ~= LDT_TYPE_LSET then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LSET_BIN(%s) Corrupted:No magic:1",
            MOD, meth, LSET_CONTROL_BIN );
      error( ldte.ERR_BIN_DAMAGED );
    end
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[LSET_CONTROL_BIN] ~= nil then
    
       local lsetList = topRec[LSET_CONTROL_BIN]; -- The main lset map
       local propMap = lsetList[1];
       local lsetMap  = lsetList[2];
    
       if( propMap[PM_Magic] ~= MAGIC ) or propMap[PM_LdtType] ~= LDT_TYPE_LSET then
        GP=F and warn("[ERROR EXIT]:<%s:%s>LSET_BIN<%s:%s>Corrupted:No magic:2",
              MOD, meth, LSET_CONTROL_BIN, tostring( lsetMap ));
        error( ldte.ERR_BIN_DAMAGED );
      end
    end
  end
end -- validateRecBinAndMap()

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

  GP=F and info("\n\n >>>>>>>>> API[ LSET CREATE ] <<<<<<<<<< \n\n");

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LSET field to an
  -- existing record.
  if( topRec[LSET_CONTROL_BIN] ~= nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> LSET CONTROL BIN Already Exists",
                   MOD, meth );
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.
  
  -- This will throw and error and jump out of Lua if binName is bad.
  -- NOTE: Not needed until we switch to using a SINGLE BIN for LSET.
  -- validateBinName( lsetBinName );

  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
 
  local lsetList = initializeLSetMap( topRec, lsetBinName );
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2]; 
  
  -- Set the type of this record to LDT (it might already be set)
  --record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in some settings that override our defaults
  -- (createSpec) then apply them now.
  if createSpec ~= nil then 
    adjustLSetMap( lsetMap, createSpec );
    -- Changes to the map need to be re-appended to topRec  
--     local NewlsetList = list();
--     list.append( NewlsetList, propMap );
--     list.append( NewlsetList, lsetMap );
--     topRec[LSET_CONTROL_BIN] = NewlsetList;
  end

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
                 MOD, meth , tostring(lsetMap));

  -- Sets the topRec control bin attribute to point to the 2 item list
  -- we created from InitializeLsetMap() : 
  -- Item 1 :  the property map & Item 2 : the lsetMap
  
  topRec[LSET_CONTROL_BIN] = lsetList; -- store in the record

  -- initializeLSetMap always sets lsetMap[M_StoreState] to SS_COMPACT
  -- At this point there is only one bin.
  -- This one will assign the actual record-list to topRec[binName]
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

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, false );
  local lsetList;
  local propMap;
  local lsetMap;

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[LSET_CONTROL_BIN] == nil ) then
    warn("[WARNING]: <%s:%s> LSET CONTROL BIN does not Exist:Creating",
         MOD, meth );
          
    lsetList = initializeLSetMap( topRec, lsetBinName );
    propMap     = lsetList[1]; 
    lsetMap = lsetList[2]; 
    topRec[LSET_CONTROL_BIN] = lsetList; -- store in the record
    
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLSetMap( lsetMap, createSpec );
    end
         
    -- initializeLSetMap always sets lsetMap[M_StoreState] to SS_COMPACT
    -- At this point there is only one bin
    setupNewBin( topRec, 0 ); -- set up Bin ZERO
    
  else
    lsetList = topRec[LSET_CONTROL_BIN]; -- The main lset control structure
    propMap = lsetList[1];
    lsetMap  = lsetList[2];
  end

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  -- These should already be set
  -- lsetList = topRec[LSET_CONTROL_BIN]; -- The main lset map
  -- propMap = lsetList[1];
  -- lsetMap  = lsetList[2];

  local totalCount = lsetMap[M_TotalCount];
  local itemCount = propMap[PM_ItemCount];
  
  GP=F and trace("[DEBUG]<%s:%s>Store State(%s) Total Count(%d) ItemCount(%d)",
    MOD, meth, tostring(lsetMap[M_StoreState]), totalCount, itemCount );

  if lsetMap[M_StoreState] == SS_COMPACT and
      totalCount >= lsetMap[M_ThreshHold]
  then
    GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH BEFORE INSERT", MOD, meth);
    rehashSet( topRec, lsetBinName, lsetList );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, lsetList, newValue, 1 );

  -- NOTE: the update of the TOP RECORD (LSET_CONTROL_BIN) has already
  -- been taken care of in localInsert, so we don't need to do it here.
  -- topRec[LSET_CONTROL_BIN] = lsetList;
  -- Also -- in Lua -- all data (like the maps and lists) are inked by
  -- reference -- so they do not need to be "re-updated".  However, the
  -- record itself, must have the object re-assigned to the BIN.
  
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
  GP=F and info("\n\n >>>>>>>>> API[ LSET INSERT ] <<<<<<<<<< \n\n");
  return localLSetInsert( topRec, lsetBinName, newValue, nil )
end -- lset_insert()

function lset_create_and_insert( topRec, lsetBinName, newValue, createSpec )
  GP=F and info("\n\n >>>>>>>>> API[ LSET CREATE AND INSERT ] <<<<<<<<<< \n\n");
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
  local lsetList = topRec[LSET_CONTROL_BIN];
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];  
  local binNumber = computeSetBin( searchValue, lsetMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local resultList = list();
  -- In all other cases of calling scanList, we need to reset topRec
  -- and lsetList except when checking for exists
  local result = scanList( resultList, lsetList, binList, searchValue,
                            FV_SCAN, filter, fargs);
                            
  -- result is always 0, so we'll always go to else and return 1
  -- instead we must check for resultList                         
  if list.size(resultList) == 0 then
    return 0
  else
    return 1
  end
  
end -- function localLSetExists()

-- ======================================================================
-- lset_exists() -- with and without filter
-- ======================================================================
function lset_exists( topRec, lsetBinName, searchValue )
  GP=F and info("\n\n >>>>>>>>> API[ LSET EXISTS ] <<<<<<<<<< \n\n");
  return localLSetExists( topRec, lsetBinName, searchValue, nil, nil )
end -- lset_exists()

function lset_exists_then_filter( topRec, lsetBinName, searchValue,
                                  filter, fargs )
  GP=F and info("\n\n >>>>>>>>> API[ LSET EXISTS Then FILTER] <<<<<<<<<< \n\n");
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

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Find the appropriate bin for the Search value
  local lsetList = topRec[LSET_CONTROL_BIN];
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  local binNumber = computeSetBin( searchValue, lsetMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  rc = 
    scanList(resultList,lsetList,binList,searchValue,FV_SCAN,filter,fargs);

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
  GP=F and trace("[ENTER]<%s:%s> Null SV: return all . Name(%s)",
                 MOD, meth, tostring(lsetBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  -- Find the appropriate bin for the Search value
  local lsetList = topRec[LSET_CONTROL_BIN];
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  
 
  if lsetMap[M_KeyType] == KT_ATOMIC then
	rc = simpleScanListAll(topRec, resultList, lsetList) 
  else
	rc = complexScanListAll(topRec, resultList, lsetList)
  end

  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s) Size : %d",
                 MOD, meth, tostring(resultList), list.size(resultList));

  return resultList; 
end -- function localLSetSearchAll()


-- ======================================================================
-- lset_search() -- with and without filter
-- ======================================================================
function lset_search( topRec, lsetBinName, searchValue )
  GP=F and info("\n\n >>>>>>>>> API[ LSET SEARCH ] <<<<<<<<<< \n\n");
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue
  if( searchValue == nil ) then
    -- if no search value, use the faster SCAN (searchALL)
    return localLSetSearchAll(resultList,topRec,lsetBinName,nil,nil)
  else
	  return localLSetSearch(resultList,topRec,lsetBinName,searchValue,nil,nil)
  end
end -- lset_search()

-- ======================================================================
function
lset_search_then_filter( topRec, lsetBinName, searchValue, filter, fargs )
  GP=F and info("\n\n >>>>>>>>> API[ LSET SEARCH Then FILTER] <<<<<<<<<< \n\n");
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue
  if( searchValue == nil ) then
    -- if no search value, use the faster SCAN (searchALL)
	return localLSetSearchAll(resultList,topRec,lsetBinName,filter,fargs)
  else
  	return localLSetSearch(resultList,topRec,lsetBinName,searchValue,filter,fargs)
  end
end -- lset_search_then_filter()

-- ======================================================================
-- lset_scan() -- with and without filter
-- ======================================================================
function lset_scan( topRec, lsetBinName )
  GP=F and info("\n\n >>>>>>>>> API[ LSET SCAN ] <<<<<<<<<< \n\n");
  resultList = list();
  return localLSetSearchAll(resultList,topRec,lsetBinName,nil,nil)
end -- lset_search()

function lset_scan_then_filter(topRec, lsetBinName, searchValue, filter, fargs)
  GP=F and info("\n\n >>>>>>>>> API[ LSET SCAN THEN FILTER] <<<<<<<<<< \n\n");
  resultList = list();
  return localLSetSearchAll(resultList,topRec,lsetBinName,filter,fargs)
end -- lset_search_then_filter()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) lsetBinName:
-- (*) deleteValue:
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Arguments to feed to the filter
-- ======================================================================
local function localLSetDelete( topRec, lsetBinName, deleteValue,
        filter, fargs)

  local meth = "localLSetDelete()";
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
    error( ldte.ERR_BIN_DOES_NOT_EXIST );
  end

  -- Find the appropriate bin for the Search value
  local lsetList = topRec[LSET_CONTROL_BIN];
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];
  local binNumber = computeSetBin( deleteValue, lsetMap );

  GP=F and trace("[DEBUG]<%s:%s> LSET Summary(%s)", MOD, meth,
    lsetSummaryString( lsetList ));

  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  -- Fow now, scanList() will only NULL out the element in a list, but will
  -- not collapse it.  Later, if we see that there are a LOT of nil entries,
  -- we can RESET the set and remove all of the "gas".
  rc = scanList(resultList,lsetList,binList,deleteValue,FV_DELETE,nil,nil);
  if( rc == ERR_NOT_FOUND ) then
    -- We'll jump out with a NOT FOUND error
    warn("[WARNING]<%s:%s> Object(%s) not found", MOD, meth, 
      tostring(deleteValue));
    error( ldte.ERR_NOT_FOUND );
  elseif( rc == 0 and list.size( resultList ) > 0 ) then
    -- We found something -- and marked it nil -- so update the record
    topRec[binName] = binList;
    rc = aerospike:update( topRec );
    if( rc < 0 ) then
      warn("[ERROR]<%s:%s> Error(%d) aerospike:update(toprec)",MOD,meth,rc);
      error( ldte.ERR_DELETE );
    end
  else
    warn("[WARNING]<%s:%s> Unexpected Error(%d) from scanList()",MOD,meth,rc);
    error( ldte.ERR_DELETE );
  end

  GP=F and trace("[EXIT]: <%s:%s>: Delete RC(%d) ResultList(%s)",
                 MOD, meth, rc, tostring( resultList ));

  return resultList;
end -- function localLSetDelete()

-- ======================================================================
-- lset_delete() -- with and without filter
-- Return resultList
-- (*) If successful: return deleted items (list.size( resultList ) > 0)
-- (*) If error: resultList will be an empty list.
-- ======================================================================
function lset_delete( topRec, lsetBinName, searchValue )
  GP=F and info("\n\n >>>>>>>>> API[ LSET DELETE ] <<<<<<<<<< \n\n");
  return localLSetDelete(topRec, lsetBinName, searchValue, nil, nil )
end -- lset_delete()

-- ======================================================================
function
lset_delete_then_filter( topRec, lsetBinName, searchValue, filter, fargs )
  GP=F and info("\n\n >>>>>>>>> API[ LSET DELETE Then Filter] <<<<<<<<<< \n\n");
  return localLSetDelete( topRec, lsetBinName, searchValue, filter, fargs )
end -- lset_delete_then_filter()

-- ========================================================================
-- lset_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function lset_size( topRec, lsetBinName )
  local meth = "lset_size()";

  GP=F and trace("[ENTER1]: <%s:%s> lsetBinName(%s)",
  MOD, meth, tostring(lsetBinName));

  GP=F and info("\n\n >>>>>>>>> API[ LSET SIZE ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  local lsetList = topRec[LSET_CONTROL_BIN]; -- The main lset control structure
  local propMap = lsetList[1];
  local lsetMap  = lsetList[2];

  local itemCount = propMap[PM_ItemCount];

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

  GP=F and info("\n\n >>>>>>>>> API[ LSET CONFIG ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsetBinName, true );

  local config = lsetSummary( topRec[ LSET_CONTROL_BIN ] );

  GP=F and trace("[EXIT]:<%s:%s>:config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lset_config()

-- ========================================================================
-- lset_dump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function lset_dump( topRec, binName )
  local meth = "lset_dump()";
  GP=F and trace("[ENTER]<%s:%s> ", MOD, meth)

  local lsetList = topRec[LSET_CONTROL_BIN];
  local propMap = lsetList[1]; 
  local lsetMap = lsetList[2];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lsetMap[M_Transform] ~= nil then
    transform = functionTable[lsetMap[M_Transform]];
  end

  if lsetMap[M_UnTransform] ~= nil then
    unTransform = functionTable[lsetMap[M_UnTransform]];
  end

  -- Loop through all the modulo n lset-record bins 
  local distrib = lsetMap[M_Modulo];

  GP=F and trace(" Number of Lset bins to parse: %d ", distrib)

  local tempList;
  local binList;
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    tempList = topRec[binName];
    binList = list();
    list.append( binList, binName );
    if( tempList == nil or list.size( tempList ) == 0 ) then
      list.append( binList, "EMPTY LIST")
    else
      listAppend( binList, tempList );
    end
    info("[DEBUG]<%s:%s> BIN(%s) TList(%s) B List(%s)", MOD, meth, binName,
      tostring(tempList), tostring(binList));
  end -- end for distrib list for-loop 

  GP=F and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

  return resultList; 
end -- lset_dump();

-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
