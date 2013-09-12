-- Settings for Large Stack 
-- settings_lstack.lua:  August 29, 2013
--
-- Module Marker: Keep this in sync with the stated version
local MOD="settings_lstack_2013_08_29.a"; -- the module name used for tracing

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
-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local T = {
  M_StoreMode              = 'M', -- List or Binary Mode
  M_StoreLimit             = 'S', -- Max Item Count for stack
  M_Transform              = 't', -- User's Transform function
  M_UnTransform            = 'u', -- User's UNTransform function
  M_LdrEntryCountMax       = 'e', -- Max # of entries in an LDR
  M_LdrByteEntrySize       = 's', -- Fixed Size of a binary Object in LDR
  M_LdrByteCountMax        = 'b', -- Max # of bytes in an LDR
  M_HotListMax             = 'h', -- Max Size of the Hot List
  M_HotListTransfer        = 'X', -- Amount to transfer from Hot List
  M_WarmListMax            = 'w', -- Max # of Digests in the Warm List
  M_WarmListTransfer       = 'x', -- Amount to Transfer from the Warm List
  M_ColdListMax            = 'c',-- Max # of items in a cold dir list
  M_ColdDirRecMax          = 'C' -- Max # of Cold Dir subrecs we'll have
};


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- Define a Table of Packages that hold "prepackaged" settings that a user
-- can apply -- rather than having to set each setting individually.
-- ======================================================================
local package = {};
--
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
-- Since it takes a lot to configure an lstack map for a particular app,
-- we use these named packages to set a block of values in a consistent
-- way.  That way, users need to remember just a package name, rather then
-- 20 different settings -- any one of which can create strange behavior
-- if set badly.
-- For now (June 2013), we have just some generic settings for "Standard",
-- "Debug" and "Test". The "Debug" one is special, since it sets the
-- config values artificially low so that it exercises the system.
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
function package.StandardList( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_LIST;
  ldtMap[T.M_Transform]        = nil;
  ldtMap[T.M_UnTransform]      = nil;
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.StandardList()

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
function package.TestModeList( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_LIST;
  ldtMap[T.M_Transform]        = nil;
  ldtMap[T.M_UnTransform]      = nil;
  ldtMap[T.M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.TestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- Assumes that we're storing a list of four numbers which can be 
-- compressed with the "compressTest4()" function.
-- ======================================================================
function package.TestModeBinary( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_BINARY;
  ldtMap[T.M_Transform]        = "compressTest4";
  ldtMap[T.M_UnTransform]      = "unCompressTest4";
  ldtMap[T.M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.TestModeBinary()

-- ======================================================================
-- Package = "ProdListValBinStore";
-- Specific Production Use: 
-- (*) Tuple value (5 fields of integers)
-- (*) Transforms
-- (*) Binary Storage (uses a compacted representation)
-- ======================================================================
function package.ProdListValBinStore( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_BINARY;
  ldtMap[T.M_Transform]        = "listCompress_5_18";
  ldtMap[T.M_UnTransform]      = "listUnCompress_5_18";
  ldtMap[T.M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 200; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 18;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.ProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
function package.DebugModeObject( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_LIST;
  ldtMap[T.M_Transform]        = nil;
  ldtMap[T.M_UnTransform]      = nil;
  ldtMap[T.M_StoreLimit]       = 5000; -- 5000 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeObject()


-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
function package.DebugModeList( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_LIST;
  ldtMap[T.M_Transform]        = nil;
  ldtMap[T.M_UnTransform]      = nil;
  ldtMap[T.M_StoreLimit]       = 200; -- 200 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use BINARY MODE.
-- ======================================================================
function package.DebugModeBinary( ldtMap )
  -- General LSO Parms:
  ldtMap[T.M_StoreMode]        = SM_BINARY;
  ldtMap[T.M_Transform]        = "compressTest4";
  ldtMap[T.M_UnTransform]      = "unCompressTest4";
  ldtMap[T.M_StoreLimit]       = 200; -- 200 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[T.M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[T.M_LdrByteEntrySize] = 16;  -- Byte size of a fixed size Byte Entry
  ldtMap[T.M_LdrByteCountMax]  = 65; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[T.M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[T.M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[T.M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[T.M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[T.M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[T.M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeBinary()

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
end -- applyPackage

-- ======================================================================
-- ======================================================================
-- Define the values and operations that a user can use to modify an LDT
-- control map with certain approved settings.
-- ======================================================================

-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

  function exports.use_package( ldtMap, package_name )
    info("[MODULE] INVOKE PACKAGE(%s)", package_name );
    applyPackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map
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

  function exports.set_ldr_entry_count_max( ldtMap, value )
    ldtMap[T.M_LdrEntryCountMax] = value;
  end

  function exports.set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[T.M_LdrByteEntrySize] = value;
  end

  function exports.set_ldr_byte_count_max( ldtMap, value )
    ldtMap[T.M_LdrByteCountMax]  = value;
  end

  function exports.set_hotlist_max( ldtMap, value )
    ldtMap[T.M_HotListMax]       = value;
  end

  function exports.set_hotlist_transfer( ldtMap, value )
    ldtMap[T.M_HotListTransfer]  = value;
  end

  function exports.set_warmlist_max( ldtMap, value )
    ldtMap[T.M_WarmListMax]      = value;
  end

  function exports.set_warmlist_transfer( ldtMap, value )
    ldtMap[T.M_WarmListTransfer] = value;
  end

  function exports.set_coldlist_max( ldtMap, value )
    ldtMap[T.M_ColdListMax]      = value;
  end

  function exports.set_colddir_rec_max( ldtMap, value )
    ldtMap[T.M_ColdDirRecMax]    = value;
  end

return exports;


-- settings_lstack.lua
--
-- Use:  
-- local set_lstack = require('settings_lstack')
--
-- Use the functions in this module to override default ldtMap settings.

-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
