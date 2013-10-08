-- Settings for Large List
-- settings_llist.lua:  September 05, 2013 (tjl)
--
-- Module Marker: Keep this in sync with the stated version
local MOD="settings_llist_2013_09_05.a"; -- the module name used for tracing

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

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- HashType (HT) values
local HT_STATIC  ='S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC ='D'; -- Use a DYNAMIC set of bins for hash lists

-- In this early version of SET, we distribute values among lists that we
-- -- keep in the top record.  This is the default modulo value for that list
-- -- distribution.   Later we'll switch to a more robust B+ Tree version.
-- local DEFAULT_DISTRIB = 31;
-- -- Switch from a single list to distributed lists after this amount
-- local DEFAULT_THRESHOLD = 100;
--
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LLIST LDT Record (root) Map Fields
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Put all of these values in a table:
local T = {
  R_TotalCount          = 'T',-- A count of all "slots" used in LLIST
  R_LeafCount           = 'c',-- A count of all Leaf Nodes
  R_NodeCount           = 'C',-- A count of all Nodes (including Leaves)
  R_StoreMode           = 'M',-- SM_LIST or SM_BINARY (applies to all nodes)
  R_TreeLevel           = 'l',-- Tree Level (Root::Inner nodes::leaves)
  R_KeyType             = 'k',-- Type of key (atomic, complex)
  R_KeyUnique           = 'U',-- Are Keys Unique? (boolean)
  R_TransFunc           = 't',-- Transform Func(from user to storage)
  R_UnTransFunc         = 'u',-- Reverse transform (from storage to user)
  R_StoreState          = 'S',-- Compact or Regular Storage
  R_Threshold           = 'H',-- After this#:Move from compact to tree mode
  R_KeyFunction         = 'F',-- Function to compute Key from Object
  -- Key and Object Sizes, when using fixed length (byte array stuff)
  R_KeyByteSize         = 'B',-- Fixed Size (in bytes) of Key
  R_ObjectByteSize      = 'b',-- Fixed Size (in bytes) of Object
  -- Top Node Tree Root Directory
  R_RootListMax         = 'R', -- Length of Key List (page list is KL + 1)
  R_RootByteCountMax    = 'r',-- Max # of BYTES for keyspace in the root
  R_KeyByteArray        = 'J', -- Byte Array, when in compressed mode
  R_DigestByteArray     = 'j', -- DigestArray, when in compressed mode
  R_RootKeyList         = 'K',-- Root Key List, when in List Mode
  R_RootDigestList      = 'D',-- Digest List, when in List Mode
  R_CompactList         = 'Q',--Simple Compact List -- before "tree mode"
  -- LLIST Inner Node Settings
  R_NodeListMax         = 'X',-- Max # of items in a node (key+digest)
  R_NodeByteCountMax    = 'Y',-- Max # of BYTES for keyspace in a node
  -- LLIST Tree Leaves (Data Pages)
  R_LeafListMax         = 'x',-- Max # of items in a leaf node
  R_LeafByteCountMax    = 'y' -- Max # of BYTES for obj space in a leaf
};


-- ======================================================================
-- Define a Table of Packages
-- ======================================================================
local package = {};

  -- ======================================================================
  -- This is the standard (default) configuration
  -- Package = "StandardList"
  -- ======================================================================
    function package.StandardList( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_ATOMIC; -- Atomic Keys
    ldtMap[T.R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many inserts
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.StandardList()

  -- ======================================================================
  -- Package = "TestModeNumber"
  -- ======================================================================
  function package.TestModeNumber( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Unique values only.
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeNumber()

  -- ======================================================================
  -- Package = "TestModeNumberDup"
  -- ======================================================================
  function package.TestModeNumberDup( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = false; -- allow Duplicates
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeNumberDup()

  -- ======================================================================
  -- Package = "TestModeObjectDup"
  -- ======================================================================
  function package.TestModeObjectDup( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    -- Use the special function that simply returns the value held in
    -- the object's map field "key".
    ldtMap[T.R_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = false; -- allow Duplicates
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeObjectDup()


  -- ======================================================================
  -- Package = "TestModeObject"
  -- ======================================================================
  function package.TestModeObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 10; -- Change to TREE Ops after this many inserts
    -- Use the special function that simply returns the value held in
    -- the object's map field "key".
    ldtMap[T.R_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Assume Unique Objects
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeObject()

  -- ======================================================================
  -- Package = "TestModeList"
  -- ======================================================================
  function package.TestModeList( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
    -- ldtMap[T.R_BinName] = ldtBinName;
    ldtMap[T.R_Threshold] = 2; -- Change to TREE Operations after this many inserts
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Assume Unique Objects
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
   
  end -- package.TestModeList()

  -- ======================================================================
  -- Package = "TestModeBinary"
  -- ======================================================================
  function package.TestModeBinary( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = "compressTest4";
    ldtMap[T.R_UnTransform] = "unCompressTest4";
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
    -- ldtMap[T.R_BinName] = ldtBinName;
    ldtMap[T.R_Threshold] = 2; -- Change to TREE Mode after this many ops.
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    return 0;

  end -- package.TestModeBinary( ldtMap )

  -- ======================================================================
  -- Package = "DebugModeObject"
  -- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
  -- where we sort them based on a map field called "key".
  -- ======================================================================
  function package.DebugModeObject( ldtMap )
    local meth = "package.DebugModeObject()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Atomic Keys
    ldtMap[T.R_Threshold] = 2; -- Rehash after this many have been inserted
    ldtMap[T.R_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items

    GP=E and trace("[EXIT]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    return 0;

  end -- package.DebugModeObject()


  -- ======================================================================
  -- Package = "DebugModeObjectDup"
  -- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
  -- where we sort them based on a map field called "key".
  -- ASSUME that we will support DUPLICATES.
  -- ======================================================================
  function package.DebugModeObjectDup( ldtMap )
    local meth = "package.DebugModeObjectDup()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- Atomic Keys
    ldtMap[T.R_Threshold] = 2; -- Rehash after this many have been inserted
    ldtMap[T.R_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = false; -- Assume there will be Duplicates

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items

    return 0;

  end -- package.DebugModeObjectDup()


  -- ======================================================================
  -- Package = "DebugModeList"
  -- Test the LLIST with very small numbers to force it to make LOTS of
  -- warm and close objects with very few inserted items.
  -- ======================================================================
  function package.DebugModeList( ldtMap )
    local meth = "package.DebugModeList()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_ATOMIC; -- Atomic Keys
    ldtMap[T.R_Threshold] = 10; -- Rehash after this many have been inserted
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 10; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 10;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 10;  -- Max # of items

    return 0;

  end -- package.DebugModeList()

  -- ======================================================================
  -- Package = "DebugModeBinary"
  -- Perform the Debugging style test with compression.
  -- ======================================================================
  function package.DebugModeBinary( ldtMap )
    
    -- General Parameters
    ldtMap[T.R_Transform] = "compressTest4";
    ldtMap[T.R_UnTransform] = "unCompressTest4";
    ldtMap[T.R_KeyCompare] = "debugListCompareEqual"; -- "Simple" list comp
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = 16; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_COMPLEX; -- special function for list compare.
    -- ldtMap[T.R_BinName] = ldtBinName;
    ldtMap[T.R_Threshold] = 4; -- Rehash after this many have been inserted
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    return 0;

  end -- package.DebugModeBinary( ldtMap )

  -- ======================================================================
  -- Package = "DebugModeNumber"
  -- Perform the Debugging style test with a number
  -- ======================================================================
  function package.DebugModeNumber( ldtMap )
    local meth = "package.DebugModeNumber()";
    GP=E and trace("[ENTER]<%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );
    
    -- General Parameters
    ldtMap[T.R_Transform] = nil;
    ldtMap[T.R_UnTransform] = nil;
    ldtMap[T.R_KeyCompare] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.R_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
    ldtMap[T.R_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
    -- ldtMap[T.R_BinName] = ldtBinName;
    ldtMap[T.R_Threshold] = 4; -- Rehash after this many have been inserted
    ldtMap[T.R_KeyFunction] = nil; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = true; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );

    return 0;
  end -- package.DebugModeNumber( ldtMap )
-- ======================================================================


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


-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
local exports = {}

-- ========================================================================
-- Call one of the standard (preset) packages.  This is generally safest,
-- since we have verified that the values all fit together.
-- ========================================================================
  function exports.use_package( ldtMap, package_name )
    info("[MODULE] APPLY PACKAGE(%s)", package_name );
    applyPackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map.
  -- Note that use of these individual functions may result in odd behavior
  -- if you pick strange or incompatible values.
  --
  -- TODO: Document these functions ...
  -- ======================================================================
  -- Set the initial Store State.  Usually this is Compact Mode (a separate
  -- list), but it can be set to "regular", which will make it start in
  -- "Tree Mode".
  -- Parm "value" must be either "SS_COMPACT" or "SS_REGULAR".
  function exports.set_store_state( ldtMap, value )
    ldtMap[T.R_StoreState]       = value;
  end

  -- StoreMode must be SM_LIST or SM_BINARY
  function exports.set_store_mode( ldtMap, value )
    ldtMap[T.R_StoreMode]        = value;
  end

  function exports.set_transform( ldtMap, value )
    ldtMap[T.R_Transform]        = value;
  end

  function exports.set_untransform( ldtMap, value )
    ldtMap[T.R_UnTransform]      = value;
  end

  function exports.set_store_limit( ldtMap, value )
    ldtMap[T.R_StoreLimit]       = value;
  end

  function exports.set_key_function( ldtMap, value )
    ldtMap[T.R_KeyFunction] = value;
  end

  function exports.set_binary_store_size( ldtMap, value )
    ldtMap[T.R_BinaryStoreSize] = value;
  end

  function exports.set_root_list_max( ldtMap, value )
    ldtMap[T.R_RootListMax] = value;
  end

  function exports.set_root_bytecount_max( ldtMap, value )
    ldtMap[T.R_RootByteCountMax]  = value;
  end

  function exports.set_node_list_max( ldtMap, value )
    ldtMap[T.R_NodeListMax]  = value;
  end

  function exports.set_node_bytecount_max( ldtMap, value )
    ldtMap[T.R_NodeByteCountMax]      = value;
  end

  function exports.set_leaf_list_max( ldtMap, value )
    ldtMap[T.R_LeafListMax] = value;
  end

  function exports.set_leaf_bytecount_max( ldtMap, value )
    ldtMap[T.R_LeafByteCountMax]    = value;
  end

  function exports.set_key_type( ldtMap, value )
    ldtMap[T.R_KeyType]      = value;
  end

  function exports.set_compact_list_threshold( ldtMap, value )
    ldtMap[T.R_Threshold]    = value;
  end

return exports;


-- settings_llist.lua
--
-- Use:  
-- local set_llist = require('settings_llist')
--
-- Use the functions in this module to override default ldtMap settings.
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
