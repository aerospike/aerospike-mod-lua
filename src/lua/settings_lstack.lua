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

-- ++===============++
-- || Package Names ||
-- ++===============++
-- Package Names for "pre-packaged" settings:
local PackageStandardList        = "StandardList";
local PackageProdListValBinStore = "ProdListValBinStore";
local PackageTestModeList        = "TestModeList";
local PackageTestModeBinary      = "TestModeBinary";
local PackageDebugModeObject     = "DebugModeObject";
local PackageDebugModeObjectDups = "DebugModeObjectDups";
local PackageDebugModeList       = "DebugModeList";
local PackageDebugModeBinary     = "DebugModeBinary";

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local M_StoreMode              = 'M'; -- List or Binary Mode
local M_StoreLimit             = 'S'; -- Max Item Count for stack
local M_Transform              = 't'; -- User's Transform function
local M_UnTransform            = 'u'; -- User's UNTransform function
local M_LdrEntryCountMax       = 'e'; -- Max # of entries in an LDR
local M_LdrByteEntrySize       = 's'; -- Fixed Size of a binary Object in LDR
local M_LdrByteCountMax        = 'b'; -- Max # of bytes in an LDR
local M_HotListMax             = 'h'; -- Max Size of the Hot List
local M_HotListTransfer        = 'X'; -- Amount to transfer from Hot List
local M_WarmListMax            = 'w'; -- Max # of Digests in the Warm List
local M_WarmListTransfer       = 'x'; -- Amount to Transfer from the Warm List
local M_ColdListMax            = 'c';-- Max # of items in a cold dir list
local M_ColdDirRecMax          = 'C';-- Max # of Cold Dir subrecs we'll have

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
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
local function packageStandardList( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_LIST;
  ldtMap[M_Transform]        = nil;
  ldtMap[M_UnTransform]      = nil;
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_LIST;
  ldtMap[M_Transform]        = nil;
  ldtMap[M_UnTransform]      = nil;
  ldtMap[M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- Assumes that we're storing a list of four numbers which can be 
-- compressed with the "compressTest4()" function.
-- ======================================================================
local function packageTestModeBinary( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_BINARY;
  ldtMap[M_Transform]        = "compressTest4";
  ldtMap[M_UnTransform]      = "unCompressTest4";
  ldtMap[M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- packageTestModeBinary()

-- ======================================================================
-- Package = "ProdListValBinStore";
-- Specific Production Use: 
-- (*) Tuple value (5 fields of integers)
-- (*) Transforms
-- (*) Binary Storage (uses a compacted representation)
-- ======================================================================
local function packageProdListValBinStore( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_BINARY;
  ldtMap[M_Transform]        = "listCompress_5_18";
  ldtMap[M_UnTransform]      = "listUnCompress_5_18";
  ldtMap[M_StoreLimit]       = 20000; -- 20k entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 200; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 18;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
local function packageDebugModeObject( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_LIST;
  ldtMap[M_Transform]        = nil;
  ldtMap[M_UnTransform]      = nil;
  ldtMap[M_StoreLimit]       = 5000; -- 5000 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- packageDebugModeObject()


-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
local function packageDebugModeList( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_LIST;
  ldtMap[M_Transform]        = nil;
  ldtMap[M_UnTransform]      = nil;
  ldtMap[M_StoreLimit]       = 200; -- 200 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use BINARY MODE.
-- ======================================================================
local function packageDebugModeBinary( ldtMap )
  -- General LSO Parms:
  ldtMap[M_StoreMode]        = SM_BINARY;
  ldtMap[M_Transform]        = "compressTest4";
  ldtMap[M_UnTransform]      = "unCompressTest4";
  ldtMap[M_StoreLimit]       = 200; -- 200 entries
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[M_LdrByteEntrySize] = 16;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  = 65; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[M_ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- packageDebugModeBinary()

-- ======================================================================
-- invokePackage():
-- ======================================================================
-- Search our standard package names and if the user is requesting one
-- of them -- apply it on the ldtMap.
-- Parms:
-- (*) ldtCtrl: the main LDT Control structure
-- (*) packageName;
-- ======================================================================
local function invokePackage( ldtMap, packageName )
  local meth = "invokePackage()";

  GP=E and trace("[ENTER]: <%s:%s>:: ldtCtrl(%s)::\n packageName(%s)",
    MOD, meth, tostring(ldtMap), tostring( packageName ));

  -- Figure out WHICH package we're going to deploy:
  if packageName == PackageStandardList then
      packageStandardList( ldtMap );
  elseif packageName == PackageTestModeList then
      packageTestModeList( ldtMap );
  elseif packageName == PackageTestModeBinary then
      packageTestModeBinary( ldtMap );
  elseif packageName == PackageProdListValBinStore then
      packageProdListValBinStore( ldtMap );
  elseif packageName == PackageDebugModeObject then
      packageDebugModeObject( ldtMap );
  elseif packageName == PackageDebugModeList then
      packageDebugModeList( ldtMap );
  elseif packageName == PackageDebugModeBinary then
      packageDebugModeBinary( ldtMap );
  end

  GP=E and trace("[EXIT]:<%s:%s>: ldtCtrl after Adjust(%s)",
    MOD,meth,tostring(ldtMap));
  return ldtMap;
end -- invokePackage

-- ======================================================================
-- ======================================================================
-- Define the values and operations that a user can use to modify an LDT
-- control map with certain approved settings.
-- ======================================================================

local exports = {

  function use_package( ldtMap, package_name )
    info("[MODULE] INVOKE PACKAGE(%s)", package_name );
    invokePackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map
  -- ======================================================================
  --
  -- StoreMode must be SM_LIST or SM_BINARY
  function set_store_mode( ldtMap, value )
    ldtMap[M_StoreMode]        = value;
  end

  function set_transform( ldtMap, value )
    ldtMap[M_Transform]        = value;
  end

  function set_untransform( ldtMap, value )
    ldtMap[M_UnTransform]      = value;
  end

  function set_store_limit( ldtMap, value )
    ldtMap[M_StoreLimit]       = value;
  end

  function set_ldr_entry_count_max( ldtMap, value )
    ldtMap[M_LdrEntryCountMax] = value;
  end

  function set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[M_LdrByteEntrySize] = value;
  end

  function set_ldr_byte_count_max( ldtMap, value )
    ldtMap[M_LdrByteCountMax]  = value;
  end

  function set_hotlist_max( ldtMap, value )
    ldtMap[M_HotListMax]       = value;
  end

  function set_hotlist_transfer( ldtMap, value )
    ldtMap[M_HotListTransfer]  = value;
  end

  function set_warmlist_max( ldtMap, value )
    ldtMap[M_WarmListMax]      = value;
  end

  function set_warmlist_transfer( ldtMap, value )
    ldtMap[M_WarmListTransfer] = value;
  end

  function set_coldlist_max( ldtMap, value )
    ldtMap[M_ColdListMax]      = value;
  end

  function set_colddir_rec_max( ldtMap, value )
    ldtMap[M_ColdDirRecMax]    = value;
  end

} -- end of Exports list

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
