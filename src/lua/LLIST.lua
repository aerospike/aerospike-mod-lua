-- Large Ordered List (LLIST)
-- Last Update May  20,  2013: tjl
--
-- Keep this MOD value in sync with version above
local MOD = "LlistStrawman05.20.0"; -- module name used for tracing.  
-- ======================================================================
-- The Large Ordered List is a sorted list, organized according to a Key
-- value.  It is assumed that the stored object is more complex than just an
-- atomic key value -- otherwise one of the other Large Object mechanisms
-- (e.g. Large Stack, Large Set) would be used.  The cannonical form of a
-- LLIST element is a map, which includes a KEY field and other data fields.
--
-- In this first version, we may choose to use a FUNCTION to derrive the 
-- key value from the complex object (e.g. Map).
-- In the first iteration, we will use atomic values and the fixed KEY field
-- for comparisons.
--
-- Compared to Large Stack and Large Set, the Large Ordered List is managed
-- continuously (i.e. it is kept sorted), so there is some additional
-- overhead in the storage operation (to do the insertion sort), but there
-- is reduced overhead for the retieval operation, since it is doing a
-- binary search (order log(N)) rather than scan (order N).
-- ======================================================================
-- Functions Supported
-- (*) llist_create: Create the LLIST structure in the chosen topRec bin
-- (*) llist_insert: Insert a user value (AS_VAL) into the list
-- (*) llist_search: Search the ordered list, using tree search
-- (*) llist_delete: Remove an element from the list
-- ==> The Insert, Search and Delete functions have a "Multi" option,
--     which allows the caller to pass in multiple list keys that will
--     result in multiple operations.  Multi-operations provide higher
--     performance since there can be many operations performed with
--     a single "client-server crossing".
-- (*) llist_multi_insert():
-- (*) llist_multi_search():
-- (*) llist_multi_delete():
-- ==> The Insert and Search functions have the option of passing in a
--     Transformation/Filter UDF that modifies values before storage or
--     modify and filter values during retrieval.
-- (*) llist_insert_with_udf() llist_multi_insert_with_udf():
--     Insert a user value (AS_VAL) in the ordered list, 
--     calling the supplied UDF on the value FIRST to transform it before
--     storing it.
-- (*) llist_search_with_udf, llist_multi_search_with_udf:
--     Retrieve a value from the list. Prior to fetching the
--     item, apply the transformation/filter UDF to the value before
--     adding it to the result list.  If the value doesn't pass the
--     filter, the filter returns nil, and thus it would not be added
--     to the result list.
-- ======================================================================
-- LLIST Design and Type Comments:
--
-- The LLIST value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information, a directory of records that serve as
-- B+Tree Nodes (either inner nodes or data nodes).
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Here is a sample B+ tree:  There are N keys and (N+1) pointers (digests)
-- in an inner node (including the root).  All of the data resides in the
-- leaves, and the inner nodes are just keys and pointers.
--
--                                   _________
--                                  |_30_|_60_|
--                               _/      |      \_
--                             _/        |        \_
--                           _/          |          \_
--                         _/            |            \_
--                       _/              |              \_
--                     _/                |                \_
--          ________ _/          ________|              ____\_________
--         |_5_|_20_|           |_40_|_50_|            |_70_|_80_|_90_|
--       _/   /    /          _/     |   /            /     |    |     \
--      /    /    /          /       |   |           /      |    |      \
--     /    /    /          /      _/    |          /       |    |       \
--    /    /    |          /      /      |          |       |    |        \
--+---+ +-----+ +-----+ +-----++-----++-----+ +-----+ +-----++-----+ +-------+
--|1|3| |6|7|8| |22|26| |30|39||40|46||51|55| |61|64| |70|75||83|86| |90|95|99|
--+---+ +-----+ +-----+ +-----++-----++-----+ +-----+ +-----++-----+ +-------+

-- B-tree nodes have a variable number of keys and children, subject to some
-- constraints.
-- A B-tree is a tree with root root[T] with the following properties:
-- Every node has the following fields:
-- 
-- (*)  n[x], the number of keys currently in node x.
-- For example, n[|40|50|] in the above example B-tree is 2.
-- n[|70|80|90|] is 3.
-- The n[x] keys themselves, stored in nondecreasing order:
-- key1[x] <= key2[x] <= ... <= keyn[x][x]
-- For example, the keys in |70|80|90| are ordered.
--   leaf[x], a boolean value that is:
--   True if x is a leaf and False if x is an internal node. 
--     If x is an internal node, it contains:
--       n[x]+1 pointers c1, c2, ... , cn[x], cn[x]+1 to its children.
--       For example, in the above B-tree, the root node has two keys,
--       thus three children. Leaf nodes have no children so their ci fields
--       are undefined.
--     The keys keyi[x] separate the ranges of keys stored in each subtree:
--     if ki is any key stored in the subtree with root ci[x], then
-- 
--         k1 <= key1[x] <= k2 <= key2[x] <= ... <= keyn[x][x] <= kn[x]+1. 
-- 
--     For example, everything in the far left subtree of the root is numbered
--     less than 30. Everything in the middle subtree is between 30 and 60,
--     while everything in the far right subtree is greater than 60. The same
--     property can be seen at each level for all keys in non-leaf nodes.
--     Every leaf has the same depth, which is the tree's height h. In the
--     above example, h=2.
--     There are lower and upper bounds on the number of keys a node can
--     contain. These bounds can be expressed in terms of a fixed integer
--     t >= 2 called the minimum degree of the B-tree:
--         Every node other than the root must have at least t-1 keys. Every
--     internal node other than the root thus has at least t children. If the
--     tree is nonempty, the root must have at least one key.
--         Every node can contain at most 2t-1 keys. Therefore, an internal
--     node can have at most 2t children. We say that a node is full if it
--     contains exactly 2t-1 keys. 
-- 
-- Searching a B-tree Searching a B-tree is much like searching a binary
-- search tree, only the decision whether to go "left" or "right" is replaced
-- by the decision whether to go to child 1, child 2, ..., child n[x]. The
-- following procedure, B-Tree-Search, should be called with the root node as
-- its first parameter. It returns the block where the key k was found along
-- with the index of the key in the block, or "null" if the key was not found:
-- 
-- ++=============================================================++
-- || B-Tree-Search (x, k) -- search starting at node x for key k ||
-- ++=============================================================++
--     i = 1
--     -- search for the correct child
--     while i <= n[x] and k > keyi[x] do
--         i++
--     end while
-- 
--     -- now i is the least index in the key array such that k <= keyi[x],
--     -- so k will be found here or in the i'th child
-- 
--     if i <= n[x] and k = keyi[x] then 
--         -- we found k at this node
--         return (x, i)
--     
--     if leaf[x] then return null
-- 
--     -- we must read the block before we can work with it
--     Disk-Read (ci[x])
--     return B-Tree-Search (ci[x], k)
-- 
-- ++===========================++
-- || Creating an empty B+ Tree ||
-- ++===========================++
-- 
-- To initialize a B+ Tree, we build an empty root node, which means
-- we initialize the LListMap in topRec[LdtBinName].
--
-- Recall that we maintain a compact list of N elements (for values of N
-- usually between 20 and 50).  So, we always start with a group insert.
-- In fact, we'd prefer to take our initial list, then SORT IT, then
-- load directly into a leaf with the largest key in the leaf as the
-- first Root Value.  This initial insert sets up a special case where
-- there's a key value in the root, but only a single leaf, so there must
-- be a test to create the second leaf when the search value is >= the
-- single root key value.
-- 
-- This assumes there is an allocate-node function that returns a node with
-- key, c, leaf fields, etc., and that each node has a unique "address",
-- which, in our case, is an Aerospike record digest.
-- 
-- ++===============================++
-- || Inserting a key into a B-tree ||
-- ++===============================++
-- 
-- (*) Traverse the Tree, locating the Leaf Node that would contain the
-- new entry, remembering the path from root to leaf.
-- (*) If room in leaf, insert node.
-- (*) Else, split node, propagate dividing key up to parent.
-- (*) If parent full, split parent, propogate up. Iterate
-- (*) If root full, Create new level, move root contents to new level
--     NOTE: It might be better to divide root into 3 or 4 pages, rather
--     than 2.  This will take a little more thinking.

-- ======================================================================
-- TO DO List:
-- TODO:
-- (1) Initialize Maps for Root, Nodes, Leaves
-- (2) Create Search Function
-- (3) Simple Insert (Root plus Leaf Insert)
-- (4) Node Split Insert
-- (5) Simple Delete
-- (6) Complex Insert
-- ======================================================================
-- ======================================================================
-- Aerospike SubRecord Calls:
-- newRec = aerospike:create_subrec( topRec )
-- newRec = aerospike:open_subrec( topRec, childRecDigest)
-- status = aerospike:update_subrec( topRec, childRec )
-- status = aerospike:close_subrec( topRec, childRec )
-- status = aerospike:delete_subrec( topRec, childRec ) (not yet ready)
-- digest = record.digest( childRec )
-- ======================================================================
-- For additional Documentation, please see llist_design.lua
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Table of Functions: Used for Transformation and Filter Functions.
-- This is held in UdfFunctionTable.lua.  Look there for details.
-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true;
local F=true; -- Set F (flag) to true to turn ON global print

-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- Switch from a single list to B+ Tree after this amount
local DEFAULT_THRESHHOLD = 100;

-- Use this to test for LdtMap Integrity.  Every map should have one.
local MAGIC="MAGIC";     -- the magic value for Testing LLIST integrity

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY  ='B'; -- Using a Transform function to compact values
local SM_LIST    ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
local KT_ATOMIC  ='A'; -- the set value is just atomic (number or string)
local KT_COMPLEX ='C'; -- the set value is complex. Use Function to get key.

-- Search Constants
local ST_FOUND    = 'F';
local ST_NOTFOUND = 'N';

-- Bin Names for Interior Nodes and Leaf Nodes
local NODE_CTRL_BIN = "NodeCtrlBin";
local LEAF_CTRL_BIN = "NodeCtrlBin";

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

-- Package Names
-- Standard, Test and Debug Packages
local PackageStandardList    = "StandardList";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
local PackageDebugModeList   = "DebugModeList";
local PackageDebugModeBinary = "DebugModeBinary";
local PackageDebugModeNumber = "DebugModeNumber";
local PackageProdListValBinStore = "ProdListValBinStore";

-- set up our "outside" links
local  CRC32 = require('CRC32');
local functionTable = require('UdfFunctionTable');

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- There are three main Record Types used in the LLIST Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LLIST bin,
--     including the Root Directory.
-- (*) InnerNodeRec: Interior B+ Tree nodes
-- (*) DataNodeRec: The Data Leaves
--
-- <+> Naming Conventions:
--   + All Field names (e.g. ldtMap.PageMode) begin with Upper Case
--   + All variable names (e.g. ldtMap.PageMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['NodeCtrlBin']);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>

-- ======================================================================
-- initializeLListMap:
-- ======================================================================
-- Set up the LLIST Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LLIST BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LLIST
-- behavior.  Thus this function represents the "type" LLIST MAP -- all
-- LLIST control fields are defined here.
-- The LListMap is obtained using the user's LLIST Bin Name:
-- ldtMap = topRec[ldtBinName]
-- ======================================================================
local function initializeLListMap( topRec, ldtBinName, transFunc, untransFunc,
                                  funcArgs )
  local meth = "initializeLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- The LLIST Map -- with Default Values
  -- General Tree Settings
  ldtMap = map();
  ldtMap.LdtType="LLIST";   -- Mark this as a Large Ordered List
  ldtMap.ItemCount = 0;     -- A count of all items in the LLIST
  ldtMap.TotalCount = 0;    -- A count of all "slots" used in LLIST
  ldtMap.LeafCount = 0;     -- A count of all Leaf Nodes
  ldtMap.DesignVersion = 1; -- Current version of the code
  ldtMap.Magic = "MAGIC";   -- Used to verify we have a valid map
  ldtMap.ExistSubRecDig = 0; -- Pt to the LDT "Exists" subrecord (digest)
  ldtMap.BinName = ldtBinName; -- Name of the Bin for this LLIST in TopRec
  ldtMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  ldtMap.Set = "set";       -- Default Set Name -- to be overridden by user
  ldtMap.PageMode = "List"; -- "List" or "Binary" (applies to all nodes)
  ldtMap.TreeLevel = 2;     -- Start off Lvl 2: Root plus leaves
  ldtMap.DataLeafCount = 0;
  ldtMap.InnerNodeCount = 0;
  ldtMap.KeyType = KT_ATOMIC;
  ldtMap.KeyUnique = false; -- Keys are NOT unique by default.
  ldtMap.TransFunc = transFunc; -- Name of the transform (from user to storage)
  ldtMap.UnTransFunc = untransFunc; -- Reverse transform (from storage to user)
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Amount to Move out of compact mode
  --
  -- Top Node Tree Root Directory
  ldtMap.RootDirMax = 100;
  ldtMap.KeyCountMax = 100; -- Each CtrlMap has this field
  ldtMap.KeyByteArray = 0; -- Byte Array, when in compressed mode
  ldtMap.DigestByteArray = 0; -- DigestArray, when in compressed mode
  ldtMap.KeyList = 0; -- Key List, when in List Mode
  ldtMap.DigestList = 0; -- Digest List, when in List Mode
  ldtMap.CompactList = list();
  
  -- LLIST Inner Node Settings
  ldtMap.InnerNodeEntryCountMax = 50;  -- Max # of items (key+digest)
  ldtMap.InnerNodeByteEntrySize = 11;  -- Size (in bytes) of Key obj
  ldtMap.InnerNodeByteCountMax = 2000; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap.DataPageEntryCountMax = 100;  -- Max # of items
  ldtMap.DataPageByteEntrySize = 44;  -- Size (in bytes) of data obj
  ldtMap.DataPageByteCountMax = 2000; -- Max # of BYTES per data page

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth , tostring(ldtMap));

  -- Put our new map in the record, then store the record.
  topRec[ldtBinName] = ldtMap;

  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtMap
end -- initializeLListMap

-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
local function packageStandardList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many inserts
  return 0;

end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
  return 0;
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
  return 0;
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "compressTest4";
  ldtMap.UnTransform = "unCompressTest4";
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
  return 0;

end -- packageTestModeBinary( ldtMap )

-- ======================================================================
-- Package = "ProdListValBinStore"
-- This Production App uses a compacted (transformed) representation.
-- ======================================================================
local function packageProdListValBinStore( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "listCompress_5_18";
  ldtMap.UnTransform = "listUnCompress_5_18";
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_BINARY; -- Use a Byte Array
  ldtMap.BinaryStoreSize = 4; -- Storing a single 4 byte integer
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys (a number)
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = 100; -- Rehash after this many have been inserted
  return 0;
  
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LLIST with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted
  return 0;

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "compressTest4";
  ldtMap.UnTransform = "unCompressTest4";
  ldtMap.KeyCompare = "debugListCompareEqual"; -- "Simple" list comp
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = 16; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- special function for list compare.
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted
  return 0;

end -- packageDebugModeBinary( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( ldtMap )
  local meth = "packageDebugModeNumber()";
  GP=F and trace("[ENTER]: <%s:%s>:: LdtMap(%s)",
    MOD, meth, tostring(ldtMap) );
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.KeyCompare = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = 0; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Simple Number (atomic) compare
  ldtMap.BinName = ldtBinName;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted

  GP=F and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
    MOD, meth, tostring(ldtMap) );
  return 0;
end -- packageDebugModeNumber( ldtMap )

-- ======================================================================
-- adjustLListMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LListMap.
-- Parms:
-- (*) ldtMap: the main LList Bin value
-- (*) argListMap: Map of LList Settings 
-- ======================================================================
local function adjustLListMap( ldtMap, argListMap )
  local meth = "adjustLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LListMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtMap), tostring( argListMap ));

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
          packageStandardList( ldtMap );
      elseif value == PackageTestModeList then
          packageTestModeList( ldtMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( ldtMap );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( ldtMap );
      elseif value == PackageProdListValBinStore then
          packageProdListValBinStore( ldtMap );
      elseif value == PackageDebugModeList then
          packageDebugModeList( ldtMap );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( ldtMap );
      elseif value == PackageDebugModeNumber then
          packageDebugModeNumber( ldtMap );
      end
    elseif name == "KeyType" and type( value ) == "string" then
      -- Use only valid values (default to ATOMIC if not specifically complex)
      if value == KT_COMPLEX or value == "complex" then
        ldtMap.KeyType = KT_COMPLEX;
      else
        ldtMap.KeyType = KT_ATOMIC;
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        ldtMap.StoreMode = value;
      end
    end
  end -- for each argument

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));
      
  return ldtMap
end -- adjustLListMap


-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree nodes have three bins:
-- Chunks hold the actual entries. Each LDT Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The Data List Bin ('DataListBin') -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> 'ldtControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'ldtListBin' Contents (A List holding entries)
--  ==> 'ldtBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
  
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Initialize Interior B+ Tree Nodes  (Records) |||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- initializeNodeMap( Interior Tree Nodes )
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
-- (1) nodeRec['NodeCtrlBin']: The control Map (defined here)
-- (2) nodeRec['KeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['KeyBnryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['DgstListBin']: The Data Entry List (when in list mode)
-- (5) nodeRec['DgstBnryBin']: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,5.
--
-- NOTE: For the Digests, we could potentially NOT store the Lock bits
-- and the Partition Bits -- since we force all of those to be the same,
-- we know they are all identical to the top record.  So, that would save
-- us 4 bytes PER DIGEST -- which adds up for 50 to 100 entries.
-- We would use a transformation method to transform a 20 byte value into
-- and out of a 16 byte value.
--
-- ======================================================================
local function initializeNodeMap(topRec, parentRec, nodeRec, nodeMap, ldtMap)
  local meth = "initializeNodeMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  nodeMap.RootDigest = record.digest( topRec );
  nodeMap.ParentDigest = record.digest( parentRec );
  nodeMap.PageMode = ldtMap.PageMode;
  nodeMap.Digest = record.digest( nodeRec );
  -- Note: Item Count is implicitly the KeyList size
  -- Also, Remember that Digest List Size is ONE MORE than Key List Size
  ldtMap.KeyCountMax = 100; -- Each CtrlMap has this field
  nodeMap.ByteEntrySize = ldtMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  nodeMap.ByteEntryCount = 0;  -- A count of Byte Entries
  nodeMap.ByteCountMax = ldtMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  nodeMap.Version = ldtMap.Version;
  nodeMap.LogInfo = 0;
  return 0;
end -- initializeNodeMap()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree modes have three bins:
-- Chunks hold the actual entries. Each LDT Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The Data List Bin ('DataListBin') -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> 'LdtControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'LdtListBin' Contents (A List holding entries)
--  ==> 'LdtBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
-- (1) nodeRec['NodeCtrlBin']: The control Map (defined here)
-- (2) nodeRec['KeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['KeyBnryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['DgstListBin']: The Data Entry List (when in list mode)
-- (5) nodeRec['DgstBnryBin']: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,5.
-- initializeLeafMap(): Data Leaf Nodes
-- ======================================================================
local function initializeLeafMap(topRec, parentRec, leafRec, leafMap, ldtMap)
  local meth = "initializeLeafMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  leafMap.RootDigest = record.digest( topRec );
  leafMap.ParentDigest = record.digest( parentRec );
  leafMap.PageMode = ldtMap.PageMode;
  leafMap.Digest = record.digest( leafRec );
  -- Note: Item Count is implicitly the KeyList size
  leafMap.DataListMax = 100; -- Max Number of items in List of data items
  leafMap.ByteEntrySize = ldtMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  leafMap.ByteEntryCount = 0;  -- A count of Byte Entries
  leafMap.ByteCountMax = ldtMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  leafMap.Version = ldtMap.Version;
  leafMap.LogInfo = 0;
  return 0;
end -- initializeLeafMap()

-- ======================================================================
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LLIST) Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.

-- ======================================================================
-- adjustLListMap:
-- ======================================================================
-- Using the settings supplied by the caller in the listCreate call,
-- we adjust the values in the LListMap.
-- Parms:
-- (*) ldtMap: the main List Bin value
-- (*) argListMap: Map of List Settings 
-- ======================================================================
local function adjustLListMap( ldtMap, argListMap )
  local meth = "adjustLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LListMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the listCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

-- Fill in when we have a better idea of the settings.

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));

  GP=F and trace("[EXIT]:<%s:%s>:Dir Map after Init(%s)",
      MOD,meth,tostring(ldtMap));

  return ldtMap
end -- adjustLListMap

-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getLeafMap( leafSubRec )
  local meth = "getLeafMap()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );
  return leafSubRec[LEAF_CTRL_BIN]; -- this should be a map.
end -- getLeafMap


-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getNodeMap( nodeSubRec )
  local meth = "getNodeMap()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );
  return nodeSubRec[NODE_CTRL_BIN]; -- this should be a map.
end -- getNodeMap

-- ======================================================================
-- validateTopRec( topRec, ldtMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LDT bin from the rec and check for magic
-- Return: "good" or "bad"
-- ======================================================================
local function validateTopRec( topRec, ldtMap )
  local thisMap = topRec[ldtMap.BinName];
  if thisMap.Magic == "MAGIC" then
    return "good"
  else
    return "bad"
  end
end -- validateTopRec()


-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
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
  return 0;
end -- validateBinName


-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateRecBinAndMap( topRec, ldtBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=F and trace("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( ldtBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  validateBinName( ldtBinName );

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid Bin
  -- (*) Must have a valid Map in the bin.
  --
  -- Otherwise, If "mustExist" is false, then basically we're just going
  -- to check that our bin includes MAGIC, if it is non-nil.
  if mustExist == true then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error('Base Record Does NOT exist');
    end

    -- Control Bin Must Exist
    if( topRec[ldtBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LDT BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error('LDT BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local ldtMap = topRec[ldtBinName]; -- The main ldtMap map
    if ldtMap.Magic ~= MAGIC then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LDT BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( ldtBinName ) );
      error('LDT BIN Is Corrupted (No Magic::1)');
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[ldtBinName] ~= nil then
      local ldtMap = topRec[ldtBinName];
      if ldtMap.Magic ~= MAGIC then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LDT BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( ldtBinName ) );
        error('LDT BIN Is Corrupted (No Magic::2)');
      end
    end -- if worth checking
  end -- else for must exist
  return 0;

end -- validateRecBinAndMap()


-- ======================================================================
-- local function Tree Summary( ldtMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the Tree Map
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtMap )
  local resultMap             = map();
  resultMap.SUMMARY           = "List Summary String";

  -- General Tree Settings
  resultMap.LdtType         = ldtMap.LdtType;
  resultMap.ItemCount       = ldtMap.ItemCount;
  resultMap.DesignVersion   = ldtMap.DesignVersion;
  resultMap.Magic           = ldtMap.Magic;
  resultMap.ExistSubRecDig  = ldtMap.ExistSubRecDig;
  resultMap.BinName         = ldtMap.BinName;
  resultMap.NameSpace       = ldtMap.NameSpace;
  resultMap.Set             = ldtMap.Set;
  resultMap.PageMode        = ldtMap.PageMode;
  resultMap.TreeLevel       = ldtMap.TreeLevel;
  resultMap.DataLeafCount   = ldtMap.DataLeafCount;
  resultMap.InnerNodeCount  = ldtMap.InnerNodeCount;
  resultMap.KeyType         = ldtMap.KeyType;
  resultMap.TransFunc       = ldtMap.TransFunc;
  resultMap.UnTransFunc     = ldtMap.UnTransFunc;

  -- Top Node Tree Root Directory
  resultMap.RootDirMax      = ldtMap.RootDirMax;
  resultMap.KeyByteArray    = ldtMap.KeyByteArray;
  resultMap.DigestByteArray = ldtMap.DigestByteArray;
  resultMap.KeyList         = ldtMap.KeyList;
  resultMap.DigestList      = ldtMap.DigestList;
  resultMap.CompactList     = ldtMap.CompactList;
  
  -- LLIST Inner Node Settings
  resultMap.InnerNodeEntryCountMax = ldtMap.InnerNodeEntryCountMax;
  resultMap.InnerNodeByteEntrySize = ldtMap.InnerNodeByteEntrySize;
  resultMap.InnerNodeByteCountMax  = ldtMap.InnerNodeByteCountMax;

  -- LLIST Tree Leaves (Data Pages)
  resultMap.DataPageEntryCountMax  = ldtMap.DataPageEntryCountMax;
  resultMap.DataPageByteEntrySize  = ldtMap.DataPageByteEntrySize;
  resultMap.DataPageByteCountMax   = ldtMap.DataPageByteCountMax;

  return  resultMap;
end -- ldtSummary()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
local function summarizeList( myList )
  local resultMap = map();
  resultMap.Summary = "Summary of the List";
  local listSize  = list.size( myList );
  resultMap.ListSize = listSize;
  if resultMap.ListSize == 0 then
    resultMap.FirstElement = "List Is Empty";
    resultMap.LastElement = "List Is Empty";
  else
    resultMap.FirstElement = tostring( myList[1] );
    resultMap.LastElement =  tostring( myList[ listSize ] );
  end

  return tostring( resultMap );
end -- summarizeList()

-- ======================================================================
-- rootNodeSummary( topRec, ldtMap )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function rootNodeSummary( topRec, ldtMap )
  local resultMap = ldtMap;

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( resultMap  );
end -- rootNodeSummary


-- ======================================================================
-- interiorNodeSummary( intNode )
-- ======================================================================
-- Print out interesting stats about this Interior B+ Tree Node
-- ======================================================================
local function interiorNodeSummary( intNode )
  local resultMap = intNode[NODE_CTRL_BIN];

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( resultMap  );
end -- interiorNodeSummary()


-- ======================================================================
-- leafNodeSummary( leafNode )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Leaf (Data) node
-- ======================================================================
local function leafNodeSummary( leafNode )
  local resultMap = map();
  local nodeMap = nodeRecord[LEAF_CTRL_BIN];

  return tostring( resultMap );
end -- leafNodeSummary()

-- ======================================================================
-- keyCompare: (Compare ONLY Key values, not Object values)
-- ======================================================================
-- Compare Search Key Value with KeyList, following the protocol for data
-- compare types.  Since compare uses only atomic key types (the value
-- that would be the RESULT of the extractKey() function), we can do the
-- simple compare here, and we don't need "keyType".
-- Return -1 for SV < data, 0 for SV == data, 1 for SV > data
-- Return -2 if either of the values is null
-- ======================================================================
local function keyCompare( searchKey, dataKey )
  local result = 0;
  -- For atomic types (keyType == 0), compare objects directly
  if searchKey == nil or dataKey == nil then return -2 end;
  if searchKey == dataKey then
    return 0;
  elseif searchKey < dataKey then
      return -1;
  else
    return 1;
  end
  
  return -3; -- we should never be here.
end -- keyCompare()

-- ======================================================================
-- compare: (Value Compare)
-- ======================================================================
-- Compare Search Value with data, following the protocol for data
-- compare types.
-- Return -1 for SV < data, 0 for SV == data, 1 for SV > data
-- Return -2 if either of the values is null
-- ======================================================================
local function compare( keyType, sv, data )
  local result = 0;
  -- For atomic types (keyType == 0), compare objects directly
  if sv == nil or data == nil then return -2 end;
  if keyType == KT_ATOMIC then
    if sv == data then
      return 0;
    elseif sv < data then
      return -1;
    else
      return 1;
    end
  else
    -- For complex types, we have to be more careful about using the
    -- 'KEY' field -- we must check that it exists first.
    if sv.KEY == nil or data.KEY == nil then return -2 end;
    if sv.KEY == data.KEY then
      return 0;
    elseif sv.KEY < data.KEY then
      return -1;
    else
      return 1;
    end
  end
  return -3; -- we should never be here.
end -- compare()

-- ======================================================================
-- NOTE: Can we make Root, Inner and Leaf look the same?
-- Each node must have similar fields (including NodeType)
-- structure: LevelCount
-- TopPtr = InnerNodeList[1]
-- LeafPtr = InnerNodeList[ list.size(InnerNodeList) ]
-- InnerNodeList, PositionList
--
-- node = root
-- While node not leaf
--   position = search keyList( node )
--   if savePath then addToPath( node, position ) end
--   node = getNextNode( node, position )
-- end
-- searchLeaf( node )
-- ======================================================================
-- B-Tree-Search (x, k) -- search starting at node x for key k
--     i = 1
--     -- search for the correct child
--     while i <= n[x] and k > keyi[x] do
--         i++
--     end while
-- 
--     -- now i is the least index in the key array such that k <= keyi[x],
--     -- so k will be found here or in the i'th child
-- 
--     if i <= n[x] and k = keyi[x] then 
--         -- we found k at this node
--         return (x, i)
--     
--     if leaf[x] then return null
-- 
--     -- we must read the block before we can work with it
--     Disk-Read (ci[x])
--     return B-Tree-Search (ci[x], k)


-- ======================================================================
-- searchKeyList()
-- ======================================================================
-- Search the key list, return the index of the value that is less than
-- or equal to the search value.
-- Parms:
-- (*) keyList: The list of keys
-- (*) keyType: Simple or Complex Key
-- (*) searchValue:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- ======================================================================
local function searchKeyList( keyList, keyType, searchValue )
  -- Linear scan of the KeyList.  Find the appropriate entry and return
  -- the index.
  local resultIndex = 0;
  local compareResult = 0;
  -- Do the List page mode search here
  -- Later: Split the loop search into two -- atomic and map objects
  for i = 1, list.size( keyList ), 1 do
    compareResult = compare( keyType, searchValue, keyList[i] );
    if compareResult == -2 then
      return nil -- error result.
    end
    if compareResult == 0 then
      -- Found it -- return the current index
      GP=F and trace("[FOUND KEY]: <%s:%s> : Value(%s) Index(%d)",
        MOD, meth, tostring(searchValue), tostring( i));
      return i;
    elseif compareResult  == 1 then
      GP=F and trace("[FOUND GREATER THAN]: <%s:%s> : SV(%s) V(%s) I(%d)",
        MOD, meth, tostring(searchValue), tostring( keyList[i] ), i );
        return resultList;
    end
    -- otherwise, keep looking.  We haven't passed the spot yet.
  end -- for each list item

  return 0;
end -- searchKeyList()

-- ======================================================================
-- createSearchPath: Create and initialize a search path structure so
-- that we can fill it in during our tree search.
-- ======================================================================
local function createSearchPath( )
  local sp = map();
  sp.LevelCount = 0;
  sp.DigestList = list();  -- The mechanism to open each level
  sp.PositionList = list(); -- Remember where the key was
  sp.HasRoom = list(); -- Check each level so we'll know if we have to split
  return sp;
end -- createSearchPath()

-- ======================================================================
-- updateSearchPath: Rememeber the path that we took during the search
-- so that we can retrace our steps if we need to update the rest of the
-- tree after an insert or delete (although, it's unlikely that we'll do
-- any significant tree change after a delete).
-- Parms:
-- (*) SearchPath: a map that holds all of the secrets
-- (*) nodeRec: a subrec
-- (*) position: location in the current list
-- (*) keyCount: Number of keys in the list
-- ======================================================================
local function updateSearchPath(searchPath, cMap, nodeRec, position, keyCount)
  local levelCount = searchPath.LevelCount;
  searchPath.LevelCount = levelCount + 1;
  list.append( searchPath.DigestList, record.digest( nodeRec ) );
  list.append( searchPath.PositionList, position );
  if( keyCount >= cMap.KeyCountMax ) then
    list.append( searchPath.HasRoom, false );
  else
    list.append( searchPath.HasRoom, true );
  end
  return 0;
end -- updateSearchPath()


-- ======================================================================
-- keyListInsert( keyList, newKey, digestList, newDigest ))
-- ======================================================================
-- Insert a new keyValue into a keyList, and the associated digest into
-- the digestList.
-- The caller has already verified that there's room in this list for
-- one more entry (pair).
--
local function keyListInsert( ldtMap, keyList, newKey, digestList, newDigest )
  local meth = "keyListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s), keyList(%s)",
    MOD, meth, tostring(newKey), tostring( keyList ));

  local rc = 0;

  local position = searchKeyList( keyList, ldtMap.KeyType, newKey )
  GP=F and trace("[DEBUG]:<%s:%s>:searchKeyList Returns(%d) ",
    MOD, meth, position );

  rc = insertIntoList( keyList, newKey, digestList, newDigest );

  -- Assuming there's room, Move items to the right to make room for

  -- TODO: Finish this method
  print("[!!! FINISH THIS METHOD !!! (%s) ", meth );

  
  return rc;
end -- keyListInsert()

-- ======================================================================
-- searchLeaf(): Locate the matching value(s) in the leaf node(s).
-- ======================================================================
-- Leaf Node:
-- (*) TopRec digest
-- (*) Parent rec digest
-- (*) This Rec digest
-- (*) NEXT Leaf
-- (*) PREV Leaf
-- (*) Min value is implicitly index 1,
-- (*) Max value is implicitly at index (size of list)
-- (*) Beginning of last value
-- Parms:
-- (*) topRec: 
-- (*) leafNode:
-- (*) searchPath:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchValue:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- ======================================================================
local function searchLeaf(topRec, leafNode, searchPath, ldtMap, resultList,
                          searchValue, func, fargs, flag)
  -- Linear scan of the Leaf Node (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  if ldtMap.PageMode == 0 then
    -- Do the BINARY page mode search here
    GP=F and trace("[WARNING]: <%s:%s> :BINARY MODE NOT IMPLEMENTED",
        MOD, meth, tostring(newStorageValue), tostring( resultList));
    return nil; -- TODO: Build this mode.
  else
    -- Do the List page mode search here
    -- Later: Split the loop search into two -- atomic and map objects
    local leafDataList = leafNode['DataListBin'];
    local keyType = ldtMap.KeyType;
    for i = 1, list.size( leafDataList ), 1 do
      compareResult = compare( keyType, searchValue, leafDataList[i] );
      if compareResult == -2 then
        return nil -- error result.
      end
      if compareResult == 0 then
        -- Start gathering up values
        gatherLeafListData( topRec, leafNode, ldtMap, resultList, searchValue,
          func, fargs, flag );
        GP=F and trace("[FOUND VALUES]: <%s:%s> : Value(%s) Result(%s)",
          MOD, meth, tostring(newStorageValue), tostring( resultList));
          return resultList;
      elseif compareResult  == 1 then
        GP=F and trace("[NotFound]: <%s:%s> : Value(%s)",
          MOD, meth, tostring(newStorageValue) );
          return resultList;
      end
      -- otherwise, keep looking.  We haven't passed the spot yet.
    end -- for each list item
  end -- end else list mode

  return 0;
end -- searchLeaf()


-- ======================================================================
-- treeSearch( topRec, searchPath, ldtMap, newValue, stats )
-- ======================================================================
-- Search the tree (start with the root and move down). 
-- Remember the search path from root to leaf (and positions in each
-- node) so that insert, Scan and Delete can use this to set their
-- starting positions.
-- Parms:
-- (*) topRec: The top level Aerospike Record
-- (*) searchPath: A list of maps that describe each level searched
-- (*) ldtMap: 
-- (*) searchValue:
-- Return: ST_FOUND or ST_NOTFOUND; 
-- And, implicitly, the updated searchPath Object.
local function treeSearch( topRec, searchPath, ldtMap, searchValue )
  local meth = "treeSearch()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s)",
      MOD, meth, tostring(searchValue));

  local treeLevel = ldtMap.TreeLevel;
  -- Start the loop with the special Root, then drop into each successive
  -- inner node level until we get to a LEAF NODE.  We search the leaf node
  -- differently than the inner (and root) nodes.
  local keyList = getRootKeyList( ldtMap );
  local keyCount = list.size( keyList );
  local digestList = getRootDigestList( ldtMap );
  local position = 0;
  local nodeSubRec = topRec;
  local ctrlMap;
  for i = 1, i < treeLevel, 1 do
    position = searchKeyList( ldtMap, keyList, searchValue );
    updateSearchPath( searchPath, nodeSubRec, position, keyCount );
    nodeSubRec = getTreeNodeRec( topRec, ldtMap, digestList, position );
    if i > ( treeLevel + 1 ) then
      print("[DEBUG]<%s:%s> INNER NODE", MOD, meth );
      ctrlMap = nodeSubRec[NODE_CTRL_BIN];
      keyList = ctrlMap.KeyList;
      keyCount = list.size( keyList );
      digestList = ctrlMap.DigestList;
    else -- then this is a leaf node
      print("[DEBUG]<%s:%s> LEAF NODE", MOD, meth );
    end -- if set up for next level
  end -- end for each tree level

  -- We've made it thru the inner nodes, so now search the leaf:
  -- TODO : Update searchLeaf() to do the NEW thing
  position = searchLeaf(nodeSubRec, ldtMap, searchValue );
  updateSearchPath( searchPath, nodeSubRec, position, keyCount );

  if position == 0 then
    return ST_NOTFOUND;
  else
    return ST_FOUND;
  end

  return 0;
end -- treeSearch()

-- ======================================================================
-- ======================================================================
local function populateLeaf( newLeafSubRec, ldtMap.KeyList, splitPosition )
  local meth = "populateLeaf()";
  print("[WARNING]<%s:%s> Function Not yet Implemented", MOD, meth );
  return 0;
end -- populateLeaf()

-- ======================================================================
-- listInsert()
-- Here's a general List Insert function that can be used to insert
-- keys, digests or objects.
-- ======================================================================
local function listInsert( myList, newValue, position )
  local meth = "listInsert()";
  
  local listSize = list.size( myList );
  if( position > listSize ) then
    -- Just append to the list
    list.append( myList, newValue );
  else
    -- Move elements in the list from "Position" to the end (end + 1)
    -- and then insert the new value at "Position"
    for i = listSize, position, -1  do
      myList[i+1] = myList[i];
    end -- for()
    myList[position] = newValue;
  end
end -- listInsert


-- ======================================================================
-- leafInsert()
-- Use the search position to mark the location where we have to make
-- room for the new value.
-- If we're at the end, we just append to the list.
-- ======================================================================
local function leafInsert( leafSubRec, ldtMap, newValue, position)
  local meth = "leafInsert()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  -- if "position" is 0, then we need to search for the new location of
  -- of the new value.  Otherwise, position marks the place where we are
  -- going to insert the new value -- and everything at that place (and to
  -- the right) has to slide one position to the right.
  local leafPosition = position;
  if( leafPosition == 0 ) then
  end
  local leafLevel = searchPath.TreeLevel;
  local leafPosition = searchPath.Position[leafLevel];
  local leafMap = getLeafMap( leafSubRec );

  listInsert( leafMap.ValueList, newValue, position ); -- TODO: CHECK THIS

  -- This next section is no longer needed -- done by "listInsert()".

  local listSize = list.size( ldtMap.KeyList );
  -- local i;
  if( leafPosition > listSize ) then
    -- Just append to the list
    list.append( ldtMap.KeyList, newValue );
  else
    -- Move elements in the list from "Position" to the end (end + 1)
    -- and then insert the new value at "Position"
    for i = listSize, leafPosition, -1  do
      ldtMap.KeyList[i+1] = ldtMap.KeyList[i];
    end -- for()
    ldtMap.KeyList[leafPosition] = newValue;
  end

  -- Check the code and delete the above section.
  --
  --
  return 0;
end -- leafInsert()

-- ======================================================================
-- getSplitPosition()
-- Find the right place to split the B+ Tree Leaf
-- ======================================================================
local function getLeafSplitPosition( ldtMap, leafPosition, newValue )
  local meth = "getLeafSplitPosition()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  -- This is only an approximization
  local listSize = list.size( ldtMap.KeyList );
  return listSize / 2;
end -- getLeafSplitPosition

-- ======================================================================
-- ======================================================================
local function insertParentNode(topRec,searchPath,ldtMap, nVal, nDig, curLevel)
  local meth = "insertParentNode()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  -- From our "current" level, insert this value and related digest into
  -- the parent node.

  

  return 0;
end -- insertParentNode()

-- ======================================================================
-- After splitting a leaf, reset the list so that we have just the first
-- half (the part BEFORE the split position).
-- ======================================================================
local function resetLeafAfterSplit( topRec, leafSubRec, splitPosition )
  local meth = "resetLeafAfterSplit()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  return 0;
end -- resetLeafAfterSplit

-- ======================================================================
-- splitLeafInsert()
-- We already know that there isn't enough room for the item, so we'll
-- have to split the leaf in order to insert it.
-- The searchPath position tells us the insert location in THIS leaf,
-- but, since this leaf will have to be split, it gets more complicated.
-- We split, THEN decide which leaf to use.
-- ALSO -- since we don't want to split the page in the middle of a set of
-- duplicates, we have to find the closest "key break" to the middle of
-- the page.  More thinking needed on how to handle duplicates without
-- making the page MUCH more complicated.
-- ======================================================================
local function splitLeafInsert( topRec, searchPath, ldtMap, newKey, newValue )
  local meth = "splitLeafInsert()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  local leafLevel = searchPath.TreeLevel;
  local leafPosition = searchPath.Position[leafLevel];
  local leafSubRecDigest = searchPath.DigestList[leafLevel];
  -- Open the Leaf and look inside.
  local leafSubRec = aerospike:open_subrec( topRec, leafSubRecDigest );
  local leafMap = getLeafMap( leafSubRec );

  local listSize = list.size( ldtMap.KeyList );
  local splitPosition = getLeafSplitPosition( ldtMap, leafPosition, newValue );
  local newLeafKey = getParentNodeKey( ldtMap, splitPosition );

  -- Move the section [split position, end] to the NEXT subrec and move
  -- the value at splitPosition up to the parent node.
  local newLeafSubRec = createLeaf( topRec );
  local newLeafSubRecDigest = record.digest( newSubRec );
  populateLeaf( newLeafSubRec, ldtMap.KeyList, splitPosition );

  -- Propagate the split value up to the parent (recursively).
  insertParentNode(topRec,searchPath,ldtMap,newValue,newLeafSubRec,leafLevel);

  -- Fix up the original leaf (remove the stuff that moved)
  resetLeafAfterSplit( topRec, leafSubRec, splitPosition );

  -- Now figure out WHICH of the two leaves (original or new) we have to
  -- insert the new value.
  -- Compare against the SplitValue -- if less, insert into the original leaf,
  -- and otherwise insert into the new leaf.
  local compareResult = keyCompare( newValue, newLeafKey );
  if( compareResult == -1 ) then
    -- We choose the LEFT Leaf
    leafInsert( topRec, searchPath, ldtMap, newValue )
  elseif( compareResult >= 0 ) then
    -- We choose the RIGHT (new) Leaf
  else
    -- We got some sort of goofy error.
  end

  return 0;
end -- splitLeafInsert()


-- ======================================================================
-- treeInsert( topRec, ldtMap, newValue, stats )
-- ======================================================================
-- Search the tree (start with the root and move down).  Get the spot in
-- the leaf where the insert goes.  Insert into the leaf.  Remember the
-- path on the way down, because if a leaf splits, we have to move back
-- up and potentially split the parents bottom up.
local function treeInsert( topRec, ldtMap, newValue, stats )
  local meth = "treeInsert()";
  local rc = 0;

  -- Map: Path from root to leaf, with indexes
  -- The Search path is a map of values, including lists from root to leaf
  -- showing node/list states, counts, fill factors, etc.
  local searchPath = createSearchPath();
  local status = treeSearch( topRec, searchPath, ldtMap, searchValue );

  if( status == ST_FOUND and ldtMap.KeyUnique == true ) then
    error('[Error]: Unique Key Violation');
  end
  local leafLevel = searchPath.LevelCount;
  if( searchPath.HasRoom[leafLevel] == true ) then
    local leafSubRec = searchPath.subRec[leafLevel];
    leafInsert( topRec, searchPath, ldtMap, newValue );
  else
    splitLeafInsert( topRec, searchPath, ldtMap, newValue );
  end

  return rc;
end -- treeInsert


-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the ldtMap, if present, and apply
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
-- Take the UnTransform defined in the ldtMap, if present, and apply
-- it to the dbValue, returning the unTransformed value.  If no unTransform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyUnTransform( ldtMap, storeValue )
  local returnValue = storeValue;
  if ldtMap.UnTransform ~= nil and
    functionTable[ldtMap.UnTransform] ~= nil then
    returnValue = functionTable[ldtMap.UnTransform]( storeValue );
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
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

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
local function complexScanList(ldtMap, binList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;

  local transform = nil;
  local unTransform = nil;
  if ldtMap.Transform ~= nil then
    transform = functionTable[ldtMap.Transform];
  end

  if ldtMap.UnTransform ~= nil then
    unTransform = functionTable[ldtMap.UnTransform];
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
          local itemCount = ldtMap.ItemCount;
          ldtMap.ItemCount = itemCount - 1;
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
local function simpleScanList(resultList, ldtMap, binList, value, flag,
  filter, fargs ) 
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 MOD, meth, tostring(value), list.size(binList),
                 tostring(binList))

  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  if ldtMap.Transform ~= nil then
    transform = functionTable[ldtMap.Transform];
  end

  if ldtMap.UnTransform ~= nil then
    unTransform = functionTable[ldtMap.UnTransform];
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
          local itemCount = ldtMap.ItemCount;
          ldtMap.ItemCount = itemCount - 1;
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
-- (*) ldtMap: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( resultList, ldtMap, binList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";

  GP=F and trace("[ENTER]:<%s:%s>Res(%s)Mp(%s)BL(%s)SV(%s)Fg(%s)F(%s)Frgs(%s)",
      MOD, meth, tostring( resultList), tostring(ldtMap),
      tostring(binList), tostring(searchValue), tostring(flag),
      tostring( filter ), tostring( fargs ));

  GP=F and trace("[DEBUG]:<%s:%s> KeyType(%s) A(%s) C(%s)",
      MOD, meth, tostring(ldtMap.KeyType), tostring(KT_ATOMIC),
      tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if ldtMap.KeyType == KT_ATOMIC then
    return simpleScanList(resultList, ldtMap, binList, searchValue, flag ) 
  else
    return complexScanList(resultList, ldtMap, binList, searchValue, flag ) 
  end
end -- scanList()

-- ======================================================================
-- compactListInsert( topRec, ldtMap, newValue, stats )
-- ======================================================================
-- Pass the work on to "scanList()" who is used to heavy lifting
local function compactListInsert( topRec, ldtMap, newValue, stats )
  local rc = 
    scanList( nil, ldtMap, ldtMap.CompactList, newValue, FV_INSERT, nil, nil);
  return rc;
end -- compactListInsert

-- ======================================================================
-- localInsert( topRec, ldtMap, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both convertList() and the
-- regular insert().
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtMap: The LDT control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, ldtMap, newValue, stats )
  local meth = "localInsert()";
  GP=F and trace("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));

  -- If our state is "compact", do a simple list insert, otherwise do a
  -- real tree insert.
  local insertResult = 0;
  if( ldtMap.StoreState == SS_COMPACT ) then 
    insertResult = compactListInsert( topRec, ldtMap, newValue, stats );
  else
    insertResult = treeInsert( topRec, ldtMap, newValue, stats );
  end

  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = ldtMap.ItemCount;
    local totalCount = ldtMap.TotalCount;
    ldtMap.ItemCount = itemCount + 1; -- number of valid items goes up
    ldtMap.TotalCount = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", MOD, meth, itemCount );
  end
  topRec[ldtMap.BinName] = ldtMap;

  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 MOD, meth, tostring( newValue ), tostring( ldtMap ) );
    -- No need to return anything
end -- localInsert


-- ======================================================================
-- convertList( topRec, ldtBinName, ldtMap )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we take our simple list and then insert into
-- the B+ Tree.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) ldtBinName
-- (*) ldtMap
-- ======================================================================
local function convertList( topRec, ldtBinName, ldtMap )
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !! CONVERT LIST !! ", MOD, meth );
  GP=F and trace("[ENTER]:<%s:%s> !! CONVERT LIST !! ", MOD, meth );

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
  ldtMap.StoreState = SS_REGULAR; -- now in "regular" (modulo) mode

  -- Rebuild. Take the compact list and insert it into the tree.
  -- The good way to do it is to sort the items and put them into a leaf
  -- in sorted order.  The simple way is to insert each one into the tree.
  -- Start with the SIMPLE way.
  -- TODO: Change this to build the tree in one operation.
  for i = 1, list.size(listCopy), 1 do
    treeInsert( topRec, ldtMap, listCopy[i], 0 ); -- do NOT update counts.
  end

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );
  return 0;
end -- convertList()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LLIST) Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || listCreate ||
-- ======================================================================
-- Create/Initialize a Large Ordered List  structure in a bin, using a
-- single LLIST -- bin, using User's name, but Aerospike TYPE (AS_LLIST)
--
-- We will use a SINGLE MAP object, which contains control information and
-- two lists (the root note Key and pointer lists).
-- (*) Namespace Name
-- (*) Set Name
-- (*) Tree Node Size
-- (*) Inner Node Count
-- (*) Data Leaf Node Count
-- (*) Total Item Count
-- (*) Storage Mode (Binary or List Mode): 0 for Binary, 1 for List
-- (*) Key Storage
-- (*) Value Storage
--
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) argList: the list of create parameters
--  (2.1) LdtBinName
--  (2.2) Namespace (just one, for now)
--  (2.3) Set
--  (2.4) LdrByteCountMax
--  (2.5) Design Version
--
function llist_create( topRec, ldtBinName, argList )
  local meth = "listCreate()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL argList",
      MOD, meth, tostring(ldtBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> ldtBinName(%s) argList(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if ldtBinName == nil  or type(ldtBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LDT BIN Name: Using default", MOD, meth );
    ldtBinName = "LdtBin";
  end

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error
  if topRec[ldtBinName] ~= nil  then
    warn("[ERROR EXIT]: <%s:%s> LDT BIN(%s) Already Exists",
      MOD, meth, tostring(ldtBinName) );
    return('LDT_BIN already exists');
  end

  -- Create and initialize the LDT MAP -- the main LDT structure
  -- initializeLListMap() also assigns the map to the record bin.
  local ldtMap = initializeLListMap( topRec, ldtBinName );

  -- If the user has passed in settings that override the defaults
  -- (the argList), then process that now.
  if argList ~= nil then
    adjustLListMap( ldtMap, argList )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  MOD,meth,tostring(ldtMap));

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
end -- function llist_create( topRec, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local localLListInsert
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Insert a value into the list (into the B+ Tree).  We will have both a
-- COMPACT storage mode and a TREE storage mode.  When in COMPACT mode,
-- the root node holds the list directly (linear search and append).
-- When in Tree mode, the root node holds the top level of the tree.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) newValue:
-- (*) createSpec:
-- =======================================================================
local function localLListInsert( topRec, ldtBinName, newValue, createSpec )
  local meth = "localLListInsert()";
  GP=F and trace("[ENTER]:<%s:%s>LLIST BIN(%s) NwVal(%s) createSpec(%s)",
    MOD, meth, tostring(ldtBinName), tostring( newValue ),tostring(createSpec));

  local ldtMap;

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- This function does not build, save or update.  It only checks.
  -- Check to see if LDT Structure (or anything) is already there.  If there
  -- is an LDT BIN present, then it MUST be valid.
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- If the record does not exist, or the BIN does not exist, then we must
  -- create it and initialize the LDT map. Otherwise, use it.
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[DEBUG]<%s:%s>LIST CONTROL BIN does not Exist:Creating",
         MOD, meth );
    ldtMap =
      initializeLListMap( topRec, ldtBinName );
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLListMap( ldtMap, createSpec );
    end
    topRec[ldtBinName] = ldtMap;
  else
    -- all there, just use it
    ldtMap = topRec[ ldtBinName ];
  end
  -- Note: We'll do the aerospike:create() at the end of this function,
  -- if needed.

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to turn our single list into a tree.
  local totalCount = ldtMap.TotalCount;
  if ldtMap.StoreState == SS_COMPACT and
    totalCount >= ldtMap.ThreshHold
  then
    convertList( topRec, ldtBinName, ldtMap );
  end
 
  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, ldtMap, newValue, 1 );

  -- All done, store the record (either CREATE or UPDATE)
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
end -- function localLListInsert()

-- =======================================================================
-- List Insert -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function llist_insert( topRec, ldtBinName, newValue )
  return localLListInsert( topRec, ldtBinName, newValue, nil )
end -- end llist_insert()

function llist_create_and_insert( topRec, ldtBinName, newValue, createSpec )
  return localLListInsert( topRec, ldtBinName, newValue, createSpec );
end -- llist_create_and_insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || localLListSearch:
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return all of the objects that match "SearchValue".
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue
-- (*) func:
-- (*) fargs:
-- ======================================================================
local function localLListSearch( topRec, ldtBinName, searchValue, func, fargs )
  local meth = "localLListSearch()";
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s) ",
      MOD, meth,tostring(searchValue) );

  -- Define our return list
  local resultList = list()
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Search the tree -- keeping track of the path from the root to the leaf
  --
  --
  local ldtMap = topRec[ldtBinName];
  local binNumber = computeSetBin( searchValue, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  rc = scanList(resultList,ldtMap,binList,searchValue,FV_SCAN,filter,fargs);
  
  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
  MOD, meth, tostring(result));
  
  return resultList;
end -- function localLListSearch() 

-- =======================================================================
-- listSearch -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type.
-- =======================================================================
function llist_search( topRec, ldtBinName, searchValue )
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> LLIST BIN(%s) searchValue(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchValue) )
  return localLListSearch( topRec, ldtBinName, searchValue, nil, nil );
end -- end llist_search()

function llist_search_with_filter(topRec,ldtBinName,searchValue,func,fargs )
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> BIN(%s) searchValue(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchValue),
    tostring(func), tostring(fargs));

  return localLListSearch( topRec, ldtBinName, searchValue, func, fargs );
end -- end llist_search_with_filter()


-- ======================================================================
-- || llist_delete ||
-- ======================================================================
-- Delete the specified item(s).
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) LdtBinName
-- (3) deleteValue: Search Structure
--
function llist_delete( topRec, ldtBinName, deleteValue )
  local meth = "listDelete()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL argList",
      MOD, meth, tostring(ldtBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> ldtBinName(%s) argList(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if ldtBinName == nil  or type(ldtBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LDT BIN Name: Using default", MOD, meth );
    ldtBinName = "LdtBin";
  end

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error
  if topRec[ldtBinName] == nil  then
    warn("[ERROR EXIT]: <%s:%s> LLIST BIN(%s) Does Not Exist!",
      MOD, meth, tostring(ldtBinName) );
    return('LLIST_BIN does not exist');
  end

  -- Call map delete to do the real work.
  local result = ldtMapDelete( topRec, ldtBinName, deleteValue );

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  MOD,meth,tostring(ldtMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function llist_delete()


-- ========================================================================
-- llist_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function llist_size( topRec, ldtBinName )
  local meth = "llist_size()";

  GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtMap = topRec[ ldtBinName ];
  local itemCount = ldtMap.ItemCount;

  GP=F and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function llist_size()

-- ========================================================================
-- llist_config() -- return the config settings
-- ========================================================================
function llist_config( topRec, ldtBinName )
  local meth = "LList_config()";

  GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local config = ldtSummary( topRec[ ldtBinName ] );

  GP=F and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, config );

  return config;
end -- function llist_config()

-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
