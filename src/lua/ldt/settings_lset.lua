-- Settings for Large Set
-- settings_lset.lua:  September 06, 2013 (tjl)
--
-- Module Marker: Keep this in sync with the stated version
local MOD="settings_lset_2013_09_06.a"; -- the module name used for tracing

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true; -- Leave this ALWAYS true (but value seems not to matter)
local F=true; -- Set F (flag) to true to turn ON global print
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print

-- ======================================================================
-- NOTE: It is VERY important that these values are kept in sync with
-- the main LDT file.  VERY!!
-- ======================================================================
-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- HashType (HT) values
local HT_STATIC  ='S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC ='D'; -- Use a DYNAMIC set of bins for hash lists

local DEFAULT_BINLIST_THRESHOLD = 4;
local DEFAULT_THRESHOLD = 20;
local DEFAULT_DISTRIB = 31;

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main Large Set Map Field Name Mapping
-- Field definitions for those fields that we'll override
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lset & lmap 
local T = {
  M_StoreMode              = 'M', -- SM_LIST or SM_BINARY
  M_StoreLimit             = 'L', -- Used for Eviction (eventually)
  M_Transform              = 't', -- Transform object to Binary form
  M_UnTransform            = 'u', -- UnTransform object from Binary form
  M_LdrEntryCountMax       = 'e', -- Max size of the LDR List
  M_LdrByteEntrySize       = 's', -- Size of a Fixed Byte Object
  M_LdrByteCountMax        = 'b', -- Max Size of the LDR in bytes
  M_StoreState             = 'S', -- Store State (Compact or List)
  M_SetTypeStore           = 'T', -- Type of the Set Store (Rec/SubRec)
  M_HashType               = 'h', -- Hash Type (static or dynamic)
  M_BinaryStoreSize        = 'B', -- Size of Object when in Binary form
  M_KeyType                = 'K', -- Key Type: Atomic or Complex
  M_Modulo 				   = 'm', -- Modulo used for Hash Function
  M_ThreshHold             = 'H', -- Threshold: Compact->Regular state
  M_KeyFunction            = 'F', -- User Supplied Key Extract Function
  M_BinListThreshold       = 'l'  -- Threshold for converting from a
                                  -- local binlist to sub-record.
};

-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- Define a Table of Packages that hold "prepackaged" settings that a user
-- can apply -- rather than having to set each setting individually.
-- ======================================================================
local package = {};

-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- Sub-Record Design, List Mode, Full Object Compare, limit 10,000 Objects
-- ======================================================================
function package.StandardList( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 50000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_COMPLEX; -- Use the FULL object
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use SubRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- not used here
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.StandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
function package.TestModeNumber( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_ATOMIC; -- Atomic Keys
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use SubRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- not used here
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeList()


-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
function package.TestModeObject( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_COMPLEX; -- either extract or tostring
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use SubRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- Use the whole object
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObject()

-- ======================================================================
-- Package = "TestModeObjectKey"
-- ======================================================================
function package.TestModeObjectKey( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_COMPLEX; -- either extract or tostring
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use SubRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = "keyExtract"; -- Defined in FunctionTable
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObjectKey()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSET with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObject( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_COMPLEX; -- Atomic Keys
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 10; -- 10 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use SubRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- no key function (use whole Obj)
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObject()

-- ======================================================================
-- Package = "DebugModeObjectTop"
-- Test the LSET with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObjectTop( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_COMPLEX; -- Atomic Keys
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used in top rec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_TOPRECORD; -- Use TOPRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- no key function (use whole Obj)
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObjectTop()

-- ======================================================================
-- Package = "DebugModeNumberTop"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumberTop( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_ATOMIC; -- Atomic Keys
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used for TopRec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_TOPRECORD; -- Use TOPRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- no key function for Numbers
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeNumber( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumber( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_KeyType]               = KT_ATOMIC; -- Atomic Keys
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used for TopRec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_SetTypeStore]          = ST_SUBRECORD; -- Use TOPRecord Store
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_KeyFunction]           = nil; -- no key function for Numbers
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeNumber( ldtMap )

-- ======================================================================
-- applyPackage():
-- ======================================================================
-- Search our standard package names and if the user is requesting one
-- of them -- apply it on the ldtMap.
-- Parms:
-- (*) ldtCtrl: the main LDT Control structure
-- (*) packageName;
-- ======================================================================
local function applyPackage( ldtMap, packageName )
  local meth = "applyPackage()";

  GP=E and trace("[ENTER]: <%s:%s>:: ldtCtrl(%s)::\n packageName(%s)",
    MOD, meth, tostring(ldtMap), tostring( packageName ));

  local ldtPackage = package[packageName];
  if( ldtPackage ~= nil ) then
    ldtPackage( ldtMap );
  end

  GP=E and trace("[EXIT]:<%s:%s>: ldtMap after Adjust(%s)",
    MOD,meth,tostring(ldtMap));

  return ldtMap;
end -- applyPackage()


-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

-- ========================================================================
-- Call one of the standard (preset) packages.  This is generally safest,
-- since we have verified that the values all fit together.
-- ========================================================================
  function exports.use_package( ldtMap, package_name )
    info("[MODULE] apply PACKAGE(%s)", package_name );
    applyPackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map.
  -- Note that use of these individual functions may result in odd behavior
  -- if you pick strange or incompatible values.
  --
  -- TODO: Document these functions ...
  -- ======================================================================
  --
  -- StoreMode must be SM_LIST or SM_BINARY
  function exports.set_store_mode( ldtMap, value )
    ldtMap[T.M_StoreMode]        = value;
  end

  function exports.set_transform( ldtMap, value )
    ldtMap[T.M_Transform]        = value;
  end

  function exports.set_untransform( ldtMap, value )
    ldtMap[T.M_UnTransform]      = value;
  end

  function exports.set_store_limit( ldtMap, value )
    ldtMap[T.M_StoreLimit]       = value;
  end

  function exports.set_unique_identifier( ldtMap, value )
    ldtMap[T.M_KeyFunction] = value;
  end

  function exports.set_ldr_entry_count_max( ldtMap, value )
    ldtMap[T.M_LdrEntryCountMax] = value;
  end

  function exports.set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[T.M_LdrByteEntrySize] = value;
  end

  function exports.set_ldr_byte_count_max( ldtMap, value )
    ldtMap[T.M_LdrByteCountMax]  = value;
  end

  function exports.set_store_state( ldtMap, value )
    ldtMap[T.M_StoreState]       = value;
  end

  function exports.set_store_type( ldtMap, value )
    ldtMap[T.M_SetTypeStore]  = value;
  end

  function exports.set_hash_type( ldtMap, value )
    ldtMap[T.M_HashType]      = value;
  end

  function exports.set_binary_store_size( ldtMap, value )
    ldtMap[T.M_BinaryStoreSize] = value;
  end

  function exports.set_key_type( ldtMap, value )
    ldtMap[T.M_KeyType]      = value;
  end

  function exports.set_hash_dir_size( ldtMap, value )
    ldtMap[T.M_Modulo]    = value;
  end

  function exports.set_compact_list_threshold( ldtMap, value )
    ldtMap[T.M_Threshold]    = value;
  end

  function exports.set_hash_cell_threshold( ldtMap, value )
    ldtMap[T.M_BinListThreshold]    = value;
  end

return exports;


-- settings_lset.lua
--
-- Use:  
-- local set_lset = require('settings_lset')
--
-- Use the functions in this module to override default ldtMap settings.

-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
