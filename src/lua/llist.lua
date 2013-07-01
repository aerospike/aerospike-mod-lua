-- Large Ordered List (llist.lua)
-- Last Update June 28,  2013: tjl
--
-- Keep this MOD value in sync with version above
local MOD = "llist::06.28.C"; -- module name used for tracing.  

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true;
local F=true; -- Set F (flag) to true to turn ON global print

-- ======================================================================
-- The Large Ordered List is a sorted list, organized according to a Key
-- value.  It is assumed that the stored object is more complex than just an
-- atomic key value -- otherwise one of the other Large Object mechanisms
-- (e.g. Large Stack, Large Set) would be used.  The cannonical form of a
-- LLIST object is a map, which includes a KEY field and other data fields.
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
-- Notice that real B+ Tree nodes have a fan-out of around 100 (maybe more,
-- maybe less, depending on key size), but that would be too hard to draw here.
--
--                                   _________
--             (Root Node)          |_30_|_60_|
--                               _/      |      \_
--                             _/        |        \_
--                           _/          |          \_
--                         _/            |            \_
--                       _/              |              \_
-- (internal nodes)    _/                |                \_
--          ________ _/          ________|              ____\_________
--         |_5_|_20_|           |_40_|_50_|            |_70_|_80_|_90_|
--        /    |    |          /     |    |           /     |    |     \
--       /     |    |         /      |    |          /      |    |     | 
--      /     /     |        /      /     |        _/     _/     |     |  
--     /     /      /       /      /      /       /      /      /      |   
--  +-^-++--^--++--^--+ +--^--++--^--++--^--+ +--^--++--^--++--^--++---^----+
--  |1|3||6|7|8||22|26| |30|39||40|46||51|55| |61|64||70|75||83|86||90|95|99|
--  +---++-----++-----+ +-----++-----++-----+ +-----++-----++-----++--------+
--  (Leaf Nodes)

-- The Root, Internal nodes and Leaf nodes have the following properties:
-- (1) The Root and Internal nodes store key values that may or may NOT
--     correspond to actual values in the leaf pages
-- (2) Key values and object values are stored in ascending order. 
--     We do not (yet) offer an ascending/descending order
-- (3) Root, Nodes and Leaves hold a variable number of keys and objects.
-- (4) Root, Nodes and Leaves may each have their own different capacity.
--
-- Searching a B+ tree is much like searching a binary
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
--     than 2.  This will take a little more thinking -- and the ability
--     to predict the future.
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
-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
-- ++====================++
-- || INTERNAL BIN NAMES || -- Local, but global to this module
-- ++====================++
-- The Top Rec LDT bin is named by the user -- so there's no hardcoded name
-- for each used LDT bin.
--
-- In the main record, there is one special hardcoded bin -- that holds
-- some shared information for all LDTs.
-- Note the 14 character limit on Aerospike Bin Names.
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local REC_LDT_CTRL_BIN    = "LDTCONTROLBIN"; -- Single bin for all LDT in rec

-- There are THREE different types of (Child) subrecords that are associated
-- with an LLIST LDT:
-- (1) Internal Node Subrecord:: Internal nodes of the B+ Tree
-- (2) Leaf Node Subrecords:: Leaf Nodes of the B+ Tree
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT subrecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN     = "SR_PROP_BIN";
--
-- The Node SubRecords (NSRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus 3 of 4 bins
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local NSR_CTRL_BIN        = "NsrControlBin";
local NSR_KEY_LIST_BIN    = "NsrKeyListBin"; -- For Var Length Keys
local NSR_KEY_BINARY_BIN  = "NsrBinaryBin";-- For Fixed Length Keys
local NSR_DIGEST_BIN      = "NsrDigestBin"; -- Digest List

-- The Leaf SubRecords (LSRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local LSR_CTRL_BIN        = "LsrControlBin";
local LSR_LIST_BIN        = "LsrListBin";
local LSR_BINARY_BIN      = "LsrBinaryBin";

-- The Existence Sub-Records (ESRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above (and that might be all)

-- ++==================++
-- || GLOBAL CONSTANTS ||
-- ++==================++
-- Each LDT defines its type in string form.
local LDT_TYPE_LLIST = "LLIST";

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.0;

-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- Switch from a single list to B+ Tree after this amount
local DEFAULT_THRESHOLD = 100;

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

-- Search Constants:: Use Numbers so that it translates to C
local ST_FOUND    =  0;
local ST_NOTFOUND = -1;

-- Values used in Compare
local CR_LESS_THAN     = -1;
local CR_EQUAL         =  0;
local CR_GREATER_THAN  =  1;
local CR_COMPARE_ERROR = -2;

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
-- come back to bite me.
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (CDIR NOT used in this context)
-- (2) As a TYPE in our own propMap[PM_RecType] field: CDIR *IS* used here.
local RT_REG  = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT  = 1; -- 0x1: Top Record (contains an LDT)
local RT_NODE = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
local RT_LEAF = 3; -- xxx: Cold Dir Subrec::Not used for set_type() 
local RT_ESR  = 4; -- 0x4: Existence Sub Record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
--
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Leaf and Node Fields (There is some overlap)
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LF_ListEntryCount       = 'L';-- # current list entries used
local LF_ListEntryTotal       = 'T';-- # total list entries allocated
local LF_ByteEntryCount       = 'B';-- # current bytes used

local ND_ListEntryCount       = 'L';-- # current list entries used
local ND_ListEntryTotal       = 'T';-- # total list entries allocated
local ND_ByteEntryCount       = 'B';-- # current bytes used

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LLIST LDT Record (root) Map Fields
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Tree Level values
local R_TotalCount          = 'T';-- A count of all "slots" used in LLIST
local R_LeafCount           = 'c';-- A count of all Leaf Nodes
local R_NodeCount           = 'C';-- A count of all Nodes (including Leaves)
local R_StoreMode           = 'M';-- SM_LIST or SM_BINARY (applies to all nodes)
local R_TreeLevel           = 'l';-- Tree Level (Root::Inner nodes::leaves)
local R_KeyType             = 'k';-- Type of key (atomic, complex)
local R_KeyUnique           = 'U';-- Are Keys Unique? (boolean)
local R_TransFunc           = 't';-- Transform Func(from user to storage)
local R_UnTransFunc         = 'u';-- Reverse transform (from storage to user)
local R_StoreState          = 'S';-- Compact or Regular Storage
local R_Threshold           = 'H';-- After this#:Move from compact to tree mode
-- Key and Object Sizes, when using fixed length (byte array stuff)
local R_KeyByteSize        = 'B';-- Fixed Size (in bytes) of Key
local R_ObjectByteSize      = 'b';-- Fixed Size (in bytes) of Object
-- Top Node Tree Root Directory
local R_RootListMax         = 'R'; -- Length of Key List (page list is KL + 1)
local R_RootByteCountMax    = 'r';-- Max # of BYTES for keyspace in the root
local R_KeyByteArray        = 'J'; -- Byte Array, when in compressed mode
local R_DigestByteArray     = 'j'; -- DigestArray, when in compressed mode
local R_RootKeyList         = 'K';-- Root Key List, when in List Mode
local R_RootDigestList      = 'D';-- Digest List, when in List Mode
local R_CompactList         = 'Q';--Simple Compact List -- before "tree mode"
-- LLIST Inner Node Settings
local R_NodeListMax         = 'X';-- Max # of items in a node (key+digest)
local R_NodeByteCountMax    = 'Y';-- Max # of BYTES for keyspace in a node
-- LLIST Tree Leaves (Data Pages)
local R_LeafListMax         = 'x';-- Max # of items in a leaf node
local R_LeafByteCountMax    = 'y';-- Max # of BYTES for obj space in a leaf
-- ------------------------------------------------------------------------
-- -- Maintain the Field letter Mapping here, so that we never have a name
-- -- collision: Obviously -- only one name can be associated with a character.
-- -- We won't need to do this for the smaller maps, as we can see by simple
-- -- inspection that we haven't reused a character.
-- ------------------------------------------------------------------------
-- A:                         a:                        0:
-- B:R_KeyByteSize            b:R_NodeByteCountSize     1:
-- C:R_NodeCount              c:R_LeafCount             2:
-- D:R_RootDigestList         d:                        3:
-- E:                         e:                        4:
-- F:                         f:                        5:
-- G:                         g:                        6:
-- H:R_Threshold              h:                        7:
-- I:                         i:                        8:
-- J:R_KeyByteArray           j:R_DigestByteArray       9:
-- K:R_RootKeyList            k:R_KeyType         
-- L:                         l:R_TreeLevel          
-- M:R_StoreMode              m:
-- N:                         n:
-- O:                         o:
-- P:                         p:
-- Q:R_CompactList            q:R_LeafByteEntrySize
-- R:R_RootListMax            r:R_RootByteCountMax      
-- S:R_StoreState             s:                        
-- T:R_TotalCount             t:R_Transform
-- U:R_KeyUnique              u:R_UnTransform
-- V:                         v:
-- W:                         w:                        
-- X:R_NodeListMax            x:R_LeafListMax           
-- Y:R_NodeByteCountMax       y:R_LeafByteCountMax
-- Z:                         z:
-- -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--
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
--   + All Field names (e.g. ldtMap[R_StoreMode]) begin with Upper Case
--   + All variable names (e.g. ldtMap[R_StoreMode]) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['NodeCtrlBin']);
--

-- ======================================================================
-- local function Tree Summary( ldtList ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the Tree Map
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtList )

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  
  local resultMap             = map();

  -- General Properties (the Properties Bin
  resultMap.SUMMARY           = "LList Summary";
  resultMap.PropBinName       = propMap[PM_BinName];
  resultMap.PropItemCount     = propMap[PM_ItemCount];
  resultMap.PropVersion       = propMap[PM_Version];
  resultMap.PropLdtType       = propMap[PM_LdtType];
  resultMap.PropEsrDigest     = propMap[PM_EsrDigest];
  resultMap.PropMagic         = propMap[PM_Magic];

  -- General Tree Settings
  resultMap.StoreMode         = ldtMap[R_StoreMode];
  resultMap.TreeLevel         = ldtMap[R_TreeLevel];
  resultMap.LeafCount         = ldtMap[R_LeafCount];
  resultMap.NodeCount         = ldtMap[R_NodeCount];
  resultMap.KeyType           = ldtMap[R_KeyType];
  resultMap.TransFunc         = ldtMap[R_TransFunc];
  resultMap.UnTransFunc       = ldtMap[R_UnTransFunc];

  -- Top Node Tree Root Directory
  resultMap.RootListMax        = ldtMap[R_RootListMax];
  resultMap.KeyByteArray       = ldtMap[R_KeyByteArray];
  resultMap.DigestByteArray    = ldtMap[R_DigestByteArray];
  resultMap.KeyList            = ldtMap[R_KeyList];
  resultMap.DigestList         = ldtMap[R_DigestList];
  resultMap.CompactList        = ldtMap[R_CompactList];
  
  -- LLIST Inner Node Settings
  resultMap.InnerNodeEntryCountMax = ldtMap[R_InnerNodeEntryCountMax];
  resultMap.InnerNodeByteEntrySize = ldtMap[R_InnerNodeByteEntrySize];
  resultMap.InnerNodeByteCountMax  = ldtMap[R_InnerNodeByteCountMax];

  -- LLIST Tree Leaves (Data Pages)
  resultMap.DataPageEntryCountMax  = ldtMap[R_DataPageEntryCountMax];
  resultMap.DataPageByteEntrySize  = ldtMap[R_DataPageByteEntrySize];
  resultMap.DataPageByteCountMax   = ldtMap[R_DataPageByteCountMax];

  return  resultMap;
end -- ldtSummary()

-- ======================================================================
-- Do the summary of the LDT, and stringify it for internal use.
-- ======================================================================
local function ldtSummaryString( ldtList )
  return tostring( ldtSummary( ldtList ) );
end -- ldtSummaryString()

-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>

-- ======================================================================
-- initializeLList:
-- ======================================================================
-- Set up the LLIST control structure with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LLIST BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LLIST
-- behavior.  Thus this function represents the "type" LLIST MAP -- all
-- LLIST control fields are defined here.
-- The LListMap is obtained using the user's LLIST Bin Name:
-- ldtMap = topRec[ldtBinName]
-- ======================================================================
local function
initializeLList( topRec, ldtBinName, transFunc, untransFunc )
  local meth = "initializeLList()";
  GP=F and trace("[ENTER]: <%s:%s>:: ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  local propMap = map();
  local ldtMap = map();
  local ldtList = list();

  -- The LLIST control structure -- with Default Values.  Note that we use
  -- two maps -- a general propery map that is the same for all LDTS (in
  -- list position ONE), and then an LDT-specific map.  This design lets us
  -- look at the general property values more easily from the Server code.
  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount] = 0; -- A count of all items in the stack
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LLIST; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = ldtBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]    = nil; -- not set yet.

  -- General Tree Settings
  ldtMap[R_TotalCount] = 0;    -- A count of all "slots" used in LLIST
  ldtMap[R_LeafCount] = 0;     -- A count of all Leaf Nodes
  ldtMap[R_NodeCount] = 0;     -- A count of all Nodes (incl leaves, excl root)
  ldtMap[R_StoreMode] = SM_LIST; -- SM_LIST or SM_BINARY (applies to all nodes)
  ldtMap[R_TreeLevel] = 1;     -- Start off Lvl 1: Root ONLY. Leaves Come l8tr
  ldtMap[R_KeyType]   = KT_ATOMIC;-- atomic or complex
  ldtMap[R_KeyUnique] = false; -- Keys are NOT unique by default.
  ldtMap[R_TransFunc] = transFunc; -- transform Func (user to storage)
  ldtMap[R_UnTransFunc] = untransFunc; -- Reverse transform (storage to user)
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD;-- Amount to Move out of compact mode

  -- Fixed Key and Object sizes -- when using Binary Storage
  ldtMap[R_KeyByteSize] = 0;   -- Size of a fixed size key
  ldtMap[R_KeyByteSize] = 0;   -- Size of a fixed size key

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  ldtMap[R_KeyByteArray] = nil; -- Byte Array, when in compressed mode
  ldtMap[R_DigestByteArray] = nil; -- DigestArray, when in compressed mode
  ldtMap[R_RootKeyList] = list();    -- Key List, when in List Mode
  ldtMap[R_RootDigestList] = list(); -- Digest List, when in List Mode
  ldtMap[R_CompactList] = list();-- Simple Compact List -- before "tree mode"
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 100;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 100;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page


  -- Put our new map in the record, then store the record.
  list.append( ldtList, propMap );
  list.append( ldtList, ldtMap ); -- ldtMap used here, not ldtList
  topRec[ldtBinName] = ldtList;

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth, ldtSummaryString(ldtList));

  return ldtList;
end -- initializeLList()

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
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many inserts
  return 0;

end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many have been inserted
 
  return 0;
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many have been inserted
  return 0;
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = "compressTest4";
  ldtMap[R_UnTransform] = "unCompressTest4";
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many have been inserted
  return 0;

end -- packageTestModeBinary( ldtMap )

-- ======================================================================
-- Package = "ProdListValBinStore"
-- This Production App uses a compacted (transformed) representation.
-- ======================================================================
local function packageProdListValBinStore( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = "listCompress_5_18";
  ldtMap[R_UnTransform] = "listUnCompress_5_18";
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_BINARY; -- Use a Byte Array
  ldtMap[R_BinaryStoreSize] = 4; -- Storing a single 4 byte integer
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys (a number)
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 100; -- Rehash after this many have been inserted
  return 0;
  
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LLIST with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( ldtMap )
  local meth = "packageDebugModeList()";
  
  GP=F and trace("[ENTER]: <%s:%s> : ldtMap(%s)",
      MOD, meth , tostring(ldtMap));

  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 4; -- Rehash after this many have been inserted
  return 0;

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = "compressTest4";
  ldtMap[R_UnTransform] = "unCompressTest4";
  ldtMap[R_KeyCompare] = "debugListCompareEqual"; -- "Simple" list comp
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = 16; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- special function for list compare.
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 4; -- Rehash after this many have been inserted
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
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_KeyCompare] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
  ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 4; -- Rehash after this many have been inserted

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
        ldtMap[R_KeyType] = KT_COMPLEX;
      else
        ldtMap[R_KeyType] = KT_ATOMIC;
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        ldtMap[R_StoreMode] = value;
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
-- initializeNode( Interior Tree Nodes )
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
--
-- TODO : MAKE THIS FOLLOW THE NEW CONVENTION for BIN NAMES
--
-- >>>>>>>>>>>>>12345678901234<<<<<< (14 char limit for Bin Names) 
-- (1) nodeRec['NsrControlBin']: The control Map (defined here)
-- (2) nodeRec['NsrKeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['NsrBinaryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['NsrDigestBin']: The Data Entry List (when in list mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,4.
--
-- NOTES:
-- (1) For the Digest Bin -- we'll be in LIST MODE for debugging, but
--     in BINARY mode for production.
-- (2) For the Digests (when we're in binary mode), we could potentially
-- save some space by NOT storing the Lock bits and the Partition Bits
-- since we force all of those to be the same,
-- we know they are all identical to the top record.  So, that would save
-- us 4 bytes PER DIGEST -- which adds up for 50 to 100 entries.
-- We would use a transformation method to transform a 20 byte value into
-- and out of a 16 byte value.
--
-- ======================================================================
local function initializeNode(topRec, nodeRec, ldtList)
  local meth = "initializeNode()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local ldtPropMap = ldtList[1];
  local ldtMap     = ldtList[2];

  -- Set up our new property and control map for this node
  local nodePropMap = map();
  local nodeMap = map();

  nodePropMap[PM_Magic] = MAGIC;
  nodePropMap[PM_EsrDigest] = ldtPropMap.EsrDigest;
  nodePropMap[PM_RecType] = RT_NODE;
  nodePropMap[PM_ParentDigest] = ldtPropMap[PM_SelfDigest];
  nodePropMap[PM_SelfDigest] = record.digest( nodeRec );
  nodeRec[SUBREC_PROP_BIN] = propMap;

  -- Notes:
  -- (1) Item Count is implicitly the KeyList size
  -- (2) All Max Limits, Key sizes and Obj sizes are in the root map
  nodeMap[ND_ListEntryCount] = 0;  -- Current # of entries in the node list
  nodeMap[ND_ListEntryTotal] = 0;  -- Total # of slots used in the node list
  nodeMap[ND_ByteEntryCount] = 0;  -- Bytes used (if in binary mode)

  -- Store the new maps in the record.
  nodeRec[SUBREC_PROP_BIN] = nodePropMap;
  nodeRec[NSR_CTRL_BIN]    = nodeMap;
  nodeRec[NSR_KEY_LIST_BIN] = list(); -- Holds the keys
  nodeRec[NSR_DIGEST_BIN] = list(); -- Holds the Digests -- the Rec Ptrs

  return 0;
end -- initializeNode()

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
-- initializeLeaf()
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially SIX bins in an Interior Tree Node Record:
-- (0) nodeRec["SR_PROP_BIN"]: The Property Map
-- (1) nodeRec['NodeCtrlBin']: The control Map (defined here)
-- (2) nodeRec['KeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['KeyBnryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['DgstListBin']: The Data Entry List (when in list mode)
-- (5) nodeRec['DgstBnryBin']: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only four fields.
-- Either Bins 0,1,2,4 or Bins 0,1,3,5.
-- Parms:
-- (*) topRec
-- (*) ldtList
-- (*) leafRec
-- ======================================================================
local function initializeLeaf(topRec, ldtList, leafRec, startCount)
  local meth = "initializeLeaf()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  local rc = 0;

  local topDigest = record.digest( topRec );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Set up the Property Map
  leafPropMap = map();
  leafPropMap[PM_Magic] = MAGIC;
  leafPropMap[PM_EsrDigest] = propMap[PM_EsrDigest]; 
  leafPropMap[PM_RecType] = RT_LEAF;
  leafPropMap[PM_ParentDigest] = topDigest;
  leafPropMap[PM_SelfDigest] = record.digest( leafRec );

  leafMap = map();
  if( ldtMap[R_StoreMode] == SM_LIST ) then
    -- List Mode
    leafMap[LF_ListEntryCount] = startCount;
    leafMap[LF_ByteEntryCount] = 0;
  else
    -- Binary Mode
    leafMap[LF_ListEntryCount] = 0;
    leafMap[LF_ByteEntryCount] = startCount;
  end

  -- Take our new structures and put them in the leaf record.
  leafRec[SUBREC_PROP_BIN] = leafPropMap;
  leafRec[LSR_CTRL_BIN] = leafMap;
  -- Note that the caller will write out the record, since there will
  -- possibly be more to do (like add data values to the object list).

  return rc;
end -- initializeLeaf()

-- ======================================================================
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LLIST) Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.


-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getLeafMap( leafSubRec )
  local meth = "getLeafMap()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );
  return leafSubRec[LSR_CTRL_BIN]; -- this should be a map.
end -- getLeafMap


-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getNodeMap( nodeRec )
  local meth = "getNodeMap()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );
  return nodeRec[NSR_CTRL_BIN]; -- this should be a map.
end -- getNodeMap

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

  -- Extract the property map and control map from the ldt bin list.
  local ldtList = topRec[ ldtBinName ];
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

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
    if ( propMap[PM_Magic] ~= MAGIC ) then
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
      if ( propMap[PM_Magic] ~= MAGIC ) then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LDT BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( ldtBinName ) );
        error('LDT BIN Is Corrupted (No Magic::2)');
      end
    end -- if worth checking
  end -- else for must exist
  return 0;

end -- validateRecBinAndMap()
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
    resultMap.LastElement =  tostring( myList[listSize] );
  end

  return tostring( resultMap );
end -- summarizeList()

-- ======================================================================
-- rootNodeSummary( topRec, ldtList )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function rootNodeSummary( topRec, ldtList )
  local resultMap = ldtList;

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( ldtSummary( ldtList )  );
end -- rootNodeSummary()


-- ======================================================================
-- interiorNodeSummary( intNode )
-- ======================================================================
-- Print out interesting stats about this Interior B+ Tree Node
-- ======================================================================
local function interiorNodeSummary( intNode )
  local resultMap = intNode[NSR_CTRL_BIN];
  local meth = "interiorNodeSummary()";

  -- Add to this -- move selected fields into resultMap and return it.
  print("[HEY!!]<%s:%s> FINISH THIS METHOD", MOD, meth );

  return tostring( resultMap  );
end -- interiorNodeSummary()


-- ======================================================================
-- leafNodeSummary( leafNode )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Leaf (Data) node
-- ======================================================================
local function leafNodeSummary( leafNode )
  local resultMap = map();
  local nodeMap = nodeRecord[LSR_CTRL_BIN];

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
local function searchKeyList( ldtMap, keyList, searchValue )
  local meth = "searchKeyList()";
  local keyType = ldtMap[R_KeyType];

  -- Linear scan of the KeyList.  Find the appropriate entry and return
  -- the index.  Binary Search will come later.
  local resultIndex = 0;
  local compareResult = 0;
  -- Do the List page mode search here
  -- Later: Split the loop search into two -- atomic and map objects
  for i = 1, list.size( keyList ), 1 do
    compareResult = compare( keyType, searchValue, keyList[i] );
    if compareResult == CR_COMPARE_ERROR then
      return nil -- error result.
    end
    if compareResult == CR_EQUAL then
      -- Found it -- return the current index
      GP=F and trace("[FOUND KEY]: <%s:%s> : Value(%s) Index(%d)",
        MOD, meth, tostring(searchValue), tostring( i));
      return i;
    elseif compareResult  == CR_GREATER_THAN then
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

  print("NOTE!! :: Must use different counts for Root, Nodes and Leaves");
  -- RootListMax, NodeListMax, LeafListMax

  local levelCount = searchPath.LevelCount;
  searchPath.LevelCount = levelCount + 1;
  list.append( searchPath.DigestList, record.digest( nodeRec ) );
  list.append( searchPath.PositionList, position );
  if( keyCount >= cMap.NodeListMax ) then
    list.append( searchPath.HasRoom, false );
  else
    list.append( searchPath.HasRoom, true );
  end
  return 0;
end -- updateSearchPath()

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
  if ldtMap[R_StoreMode] == 0 then
    -- Do the BINARY page mode search here
    GP=F and trace("[WARNING]: <%s:%s> :BINARY MODE NOT IMPLEMENTED",
        MOD, meth, tostring(newStorageValue), tostring( resultList));
    return nil; -- TODO: Build this mode.
  else
    -- Do the List page mode search here
    -- Later: Split the loop search into two -- atomic and map objects
    local leafDataList = leafNode['DataListBin'];
    local keyType = ldtMap[R_KeyType];
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
-- Get the tree node (record) the corresponds to the stated position.
-- ======================================================================
local function  getTreeNodeRec( topRec, ldtMap, digestList, position )
  local rec = aerospike:open_subrec( topRec, digestList[position] );
  return rec;
end -- getTreeNodeRec()

-- ======================================================================
-- treeSearch( topRec, searchPath, ldtList, newValue, stats )
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
-- Return: ST_FOUND(0) or ST_NOTFOUND(-1)
-- And, implicitly, the updated searchPath Object.
local function treeSearch( topRec, searchPath, ldtList, searchValue )
  local meth = "treeSearch()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s)",
      MOD, meth, tostring(searchValue));

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  local treeLevel = ldtMap[R_TreeLevel];

  -- Start the loop with the special Root, then drop into each successive
  -- inner node level until we get to a LEAF NODE.  We search the leaf node
  -- differently than the inner (and root) nodes, since they have OBJECTS
  -- and not keys.  To search a leaf we must compute the key (from the object)
  -- before we do the compare.
  local keyList = getRootKeyList( ldtMap );
  local keyCount = list.size( keyList );
  local digestList = getRootDigestList( ldtMap );
  local position = 0;
  local nodeRec = topRec;
  local nodeMap;
  for i = 1, treeLevel, 1 do
    position = searchKeyList( ldtMap, keyList, searchValue );
    updateSearchPath( searchPath, nodeRec, position, keyCount );
    nodeRec = aerospike:open_subrec( topRec, digestList[position] );
    if i > ( treeLevel + 1 ) then
      nodeMap = nodeRec[NSR_CTRL_BIN];
      propMap = nodeRec[SUBREC_PROP_BIN];
      print("[DEBUG]<%s:%s> INNER NODE: Type(%s)",
          MOD, meth, propMap[PM_);
      keyList = nodeRec[NSR_KEY_LIST_BIN];
      keyCount = list.size( keyList );
      digestList = nodeRec[NSR_DIGEST_BIN]; 
      print("[DEBUG]<%s:%s> NOT YET FINISHED!!!!  ", MOD, meth );
    else -- then this is a leaf node
      print("[DEBUG]<%s:%s> LEAF NODE", MOD, meth );

      print("[DEBUG]<%s:%s> NOT YET FINISHED!!!!  ", MOD, meth );
    end -- if set up for next level
  end -- end for each tree level

  -- We've made it thru the inner nodes, so now search the leaf:
  -- TODO : Update searchLeaf() to do the NEW thing
  position = searchLeaf(nodeRec, ldtMap, searchValue );
  updateSearchPath( searchPath, nodeRec, position, keyCount );

  if position == 0 then
    rc = ST_NOTFOUND;
  else
    rc = ST_FOUND;
  end

  GP=F and trace("[EXIT]: <%s:%s> searchValue(%s) Result(%d)",
      MOD, meth, tostring(searchValue), rc );

  return rc;
end -- treeSearch()

-- ======================================================================
-- ======================================================================
local function populateLeaf( newLeafSubRec, keyList, splitPosition )
  local meth = "populateLeaf()";

  print("[WARNING]<%s:%s> Function Not yet Implemented", MOD, meth );

  return 0;
end -- populateLeaf()

-- ======================================================================
-- listInsert()
-- General List Insert function that can be used to insert
-- keys, digests or objects.
-- ======================================================================
local function listInsert( myList, newValue, position )
  local meth = "listInsert()";
  rc = 0;
  
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

  return rc;
end -- listInsert()

-- ======================================================================
-- firstLeafInsert()
-- Very Simple -- this is the FIRST insert of this leaf, so nothing to do
-- but get the list and stuff in the value.  In fact, we can even init
-- the leaf here -- no need to do it before hand.
-- Parms:
-- (*) leafRec:  Ptr to the subrec
-- (*) newValue: Insert this value into the leaf record
-- Return: status of the subrec update call
-- ======================================================================
local function firstLeafInsert( topRec, ldtList, leafRec, newValue )
  local meth = "firstLeafInsert()";

  -- TODO : Assuming LIST MODE for now -- will change later to do BYTE also
  -- TODO : Evolve to do BOTH list and Binary modes
  initializeLeaf( topRec, ldtList, leafRec, 1);

  local objectList = list(); -- Create the Object list for this leaf
  list.append( objectList, newValue );

  leafRec[ LSR_LIST_BIN ] = objectList;

  rc = aerospike:update_subrec( leafRec );

  return rc;
end -- leafInsert()

-- ======================================================================
-- leafInsert()
-- Use the search position to mark the location where we have to make
-- room for the new value.
-- If we're at the end, we just append to the list.
-- ======================================================================
local function leafInsert( leafRec, ldtMap, newValue, position)
  local meth = "leafInsert()";
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  -- if "position" is 0, then we need to search for the new location of
  -- of the new value.  Otherwise, position marks the place where we are
  -- going to insert the new value -- and everything at that place (and to
  -- the right) has to slide one position to the right.
  local leafPosition = position;
  if( leafPosition == 0 ) then
    print("[WARNING]<%s:%s> Function REALLY Not Complete", MOD, meth );
  end

  local leafLevel = searchPath.TreeLevel;
  local leafPosition = searchPath.Position[leafLevel];
  local leafList = leafRec[LSR_LIST_BIN];
  local leafmap =  leafRec[LSR_CTRL_BIN];

  listInsert( leafList, newValue, position ); -- TODO: CHECK THIS

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
  local listSize = list.size( ldtMap[R_RootKeyList] );
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
-- Create a new Leaf Page and initialize it.
-- Parms:
-- (*) topRec: The main AS Record holding the LDT
-- NOTE: Remember that we must create an ESR when we create the first leaf
-- ======================================================================
local function createLeaf( topRec )
    local rc = 0;
    local leafRec = aerospike:create_subrec( topRec );

    -- TODO:: If this is the FIRST leaf, then we must create an ESR.
        local esrDigest = record.digest( esr );
          local topDigest = record.digest( topRec );

end

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
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );
  print("[WARNING]<%s:%s> Function Not Complete", MOD, meth );

  local leafLevel = searchPath.TreeLevel;
  local leafPosition = searchPath.Position[leafLevel];
  local leafSubRecDigest = searchPath.DigestList[leafLevel];
  -- Open the Leaf and look inside.
  local leafSubRec = aerospike:open_subrec( topRec, leafSubRecDigest );
  local leafMap = getLeafMap( leafSubRec );

  local listSize = list.size( ldtMap[R_RootKeyList] );
  local splitPosition = getLeafSplitPosition( ldtMap, leafPosition, newValue );
  local newLeafKey = getParentNodeKey( ldtMap, splitPosition );

  -- Move the section [split position, end] to the NEXT subrec and move
  -- the value at splitPosition up to the parent node.
  local newLeafSubRec = createLeaf( topRec );
  local newLeafSubRecDigest = record.digest( newSubRec );
  populateLeaf( newLeafSubRec, ldtMap[R_RootKeyList], splitPosition );

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
-- Assume atomic objects for now
-- ======================================================================
local function getKeyValue( ldtMap, value )
  if( ldtMap[R_KeyType] == KT_ATOMIC ) then
    return value;
  else
    error("Can't Compute Key for Complex Yet!!");
  end
end

-- ======================================================================
-- firstTreeInsert( topRec, ldtList, newValue, stats )
-- ======================================================================
-- For the VERY FIRST INSERT, we don't need to search.  We just put the
-- first key in the root, and we allocate TWO leaves: the left leaf for
-- values LESS THAN the first value, and the right leaf for values
-- GREATER THAN OR EQUAL to the first value.
-- Parms:
-- (*) topRec
-- (*) ldtList
-- (*) newValue
-- (*) stats: bool: When true, we update stats
local function firstTreeInsert( topRec, ldtList, newValue, stats )
  local meth = "splitLeafInsert()";
  GP=F and trace("[ENTER]<%s:%s>LdtSummary(%s) newValue(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue) );

  local rc = 0;
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  local rootKeyList = ldtMap[R_RootKeyList];
  local rootDigestList = ldtMap[R_RootDigestList];
  local keyValue = getKeyValue( newValue );

  -- Insert our very firsts key into the root directory (no search needed)
  list.append( rootKeyList, keyValue );

  -- Create two leaves -- Left and Right. Initialize them.  Then
  -- insert our new value into the RIGHT one.
  local leftLeafRec = createLeafRec( topRec );
  local leftLeafDigest = record.digest( leftLeaf );

  local rightLeafRec = createLeafRec( topRec );
  local rightLeafDigest = record.digest( rightLeaf );

  list.append( rootDigestList, leftLeafDigest );
  list.append( rootDigestList, rightLeafDigest );

  -- Insert the value and update the subRec
  firstLeafInsert( topRec, ldtList, rightLeafRec, newValue );

  if( stats == true ) then
    local itemCount = propMap[PM_ItemCount];
    propMap[PM_ItemCount] = itemCount + 1;
  end

  -- Note: The caller will update the top record.

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) newValue(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue) );
  return rc;

end -- firstTreeInsert()

-- ======================================================================
-- treeInsert( topRec, ldtList, newValue, stats )
-- ======================================================================
-- Search the tree (start with the root and move down).  Get the spot in
-- the leaf where the insert goes.  Insert into the leaf.  Remember the
-- path on the way down, because if a leaf splits, we have to move back
-- up and potentially split the parents bottom up.
-- Parms:
-- (*) topRec
-- (*) ldtList
-- (*) newValue
-- (*) stats: bool: When true, we update stats
local function treeInsert( topRec, ldtList, newValue, stats )
  local meth = "treeInsert()";
  local rc = 0;
  
  GP=F and trace("[ENTER]<%s:%s>LdtSummary(%s) newValue(%s) stats(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue), tostring(stats));

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- For the VERY FIRST INSERT, we don't need to search.  We just put the
  -- first key in the root, and we allocate TWO leaves: the left leaf for
  -- values LESS THAN the first value, and the right leaf for values
  -- GREATER THAN OR EQUAL to the first value.
  -- Note that later -- when we do a batch insert -- this will be smarter.
  if( ldtMap[R_TreeLevel] == 1 ) then
    firstTreeInsert( topRec, ldtList, newValue, stats );
  else
    -- It's a real insert -- so, Search first, then insert
    -- Map: Path from root to leaf, with indexes
    -- The Search path is a map of values, including lists from root to leaf
    -- showing node/list states, counts, fill factors, etc.
    local searchPath = createSearchPath();
    local status = treeSearch( topRec, searchPath, ldtList, searchValue );

    if( status == ST_FOUND and ldtMap[R_KeyUnique] == true ) then
      error('[Error]: Unique Key Violation');
    end
    local leafLevel = searchPath.LevelCount;
    if( searchPath.HasRoom[leafLevel] == true ) then
      local leafSubRec = searchPath.subRec[leafLevel];
      -- Regular Leaf Insert
      rc = leafInsert( topRec, searchPath, ldtList, newValue );
    else
      -- Split first, then insert.  This split can potentially propagate all
      -- the way up the tree to the root. This is potentially a big deal.
      rc = splitLeafInsert( topRec, searchPath, ldtList, newValue );
    end
  end

  -- All of the subrecords were written out in the respective insert methods,
  -- so if all went well, we'll now update the top record. Otherwise, we
  -- will NOT udate it.
  if( rc == 0 ) then
    rc = aerospike:update( topRec );
  else
    warn("[ERROR]<%s:%s>Insert Error::LdtSummary(%s) newValue(%s) stats(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue), tostring(stats));
    error("[INSERT ERROR]:: Internal Error on insert");
  end

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) newValue(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue) );
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
  if ldtMap[R_UnTransform] ~= nil and
    functionTable[ldtMap[R_UnTransform]] ~= nil then
    returnValue = functionTable[ldtMap[R_UnTransform]]( storeValue );
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
-- (*) objList: the list of values from the record
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
local function complexScanList(ldtList, objList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  local transform = nil;
  local unTransform = nil;

  if ldtMap[R_Transform] ~= nil then
    transform = functionTable[ldtMap[R_Transform]];
  end

  if ldtMap[R_UnTransform] ~= nil then
    unTransform = functionTable[ldtMap[R_UnTransform]];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( objList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(objList[i]));
    if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
      resultValue = unTransformComplexCompare(unTransform, objList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          objList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_temCount];
          propMap[PM_ItemCount] = itemCount - 1;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result
        return resultValue;
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
  return nil;
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) objList: the list of values from the record
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
local function simpleScanList(resultList, ldtList, objList, value, flag,
  filter, fargs ) 
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 MOD, meth, tostring(value), list.size(objList),
                 tostring(objList))
  local rc = 0;

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  if ldtMap[R_Transform] ~= nil then
    transform = functionTable[ldtMap[R_Transform]];
  end

  if ldtMap[R_UnTransform] ~= nil then
    unTransform = functionTable[ldtMap[R_UnTransform]];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( objList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(objList[i]));
    if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
      resultValue = unTransformSimpleCompare(unTransform, objList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          objList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
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
    list.append( objList, storeValue );
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
-- (*) objList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( resultList, ldtList, objList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";
  --
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  GP=F and trace("[ENTER]<%s:%s>RL(%s)Mp(%s)OL(%s)SV(%s)Fg(%s)Fr(%s)Args(%s)",
      MOD, meth, tostring( resultList), tostring(ldtMap),
      tostring(objList), tostring(searchValue), tostring(flag),
      tostring( filter ), tostring( fargs ));

  GP=F and trace("[DEBUG]:<%s:%s> KeyType(%s) A(%s) C(%s)",
      MOD, meth, tostring(ldtMap[R_KeyType]), tostring(KT_ATOMIC),
      tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if ldtMap[R_KeyType] == KT_ATOMIC then
    return simpleScanList(resultList, ldtList, objList, searchValue, flag ) 
  else
    return complexScanList(resultList, ldtList, objList, searchValue, flag ) 
  end
end -- scanList()

-- ======================================================================
-- compactListInsert( topRec, ldtList, newValue, stats )
-- ======================================================================
-- Pass the work on to "scanList()" who is used to heavy lifting
local function compactListInsert( topRec, ldtList, newValue, stats )
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  local compactList = ldtMap[R_CompactList];

  local rc = scanList(nil,ldtList,compactList,newValue,FV_INSERT,nil,nil);
  return rc;
end -- compactListInsert()

-- ======================================================================
-- localInsert( topRec, ldtList, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both convertList() and the
-- regular insert().
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtList: The LDT control Structure
-- (*) newValue: Value to be inserted
-- (*) stats: true=Please update Counts, false=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, ldtList, newValue, stats )
  local meth = "localInsert()";
  GP=F and trace("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- If our state is "compact", do a simple list insert, otherwise do a
  -- real tree insert.
  local insertResult = 0;
  if( ldtMap[R_StoreState] == SS_COMPACT ) then 
    insertResult = compactListInsert( topRec, ldtList, newValue, stats );
  else
    insertResult = treeInsert( topRec, ldtList, newValue, stats );
  end

  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = ldtMap[R_TotalCount];
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    ldtMap[R_TotalCount] = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", MOD, meth, itemCount );
  end
  topRec[ binName ] = ldtList;

  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 MOD, meth, tostring( newValue ), tostring( ldtMap ) );
    -- No need to return anything
end -- localInsert


-- ======================================================================
-- convertList( topRec, ldtBinName, ldtList )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshold), we take our simple list and then insert into
-- the B+ Tree.
-- So -- copy out all of the items from the CompactList and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) ldtBinName
-- (*) ldtList
-- ======================================================================
local function convertList( topRec, ldtBinName, ldtList )
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !! CONVERT LIST !! ", MOD, meth );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- iterate thru the ldtMap CompactList, re-inserting each item.
  local compactList = ldtMap[R_CompactList];

  if compactList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
      MOD, meth, tostring(singleBinName));
    error('BAD COMPACT LIST for Rehash');
  end

  ldtMap[R_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode

  -- Rebuild. Take the compact list and insert it into the tree.
  -- The good way to do it is to sort the items and put them into a leaf
  -- in sorted order.  The simple way is to insert each one into the tree.
  -- Start with the SIMPLE way.
  -- TODO: Change this to build the tree in one operation.
  for i = 1, list.size( compactList ), 1 do
    treeInsert( topRec, ldtList, compactList[i], false ); --do NOT update counts
  end

  -- Now, release the compact list we were using.
  -- TODO: Figure out exactly how Lua releases storage
  compactList = nil;
  ldtMap[R_CompactList] = nil; -- Release the list.  Does this work??

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );
  return 0;
end -- convertList()

-- ======================================================================
-- Given the searchPath result from treeSearch(), Scan the leaves for all
-- values that satisfy the searchPredicate and the filter.
-- ======================================================================
local function 
treeScan(resultList, topRec, searchPath, ldtList, searchValue, func, fargs )
  local meth = "treeScan()";
  print("[HEY!!!]<%s:%s> FUNCTION NOT YET IMPLEMENTED!!!", MOD, meth );

  return 0;

end -- treeScan()

-- ======================================================================
-- Perform the delete of the delete value
-- ======================================================================
local function localDelete( topRec, ldtBinName, deleteValue )
end -- localDelete()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||       Large Ordered List (LLIST) Main Functions        |||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
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
  -- initializeLList() also assigns the map to the record bin.
  local ldtList = initializeLList( topRec, ldtBinName, nil, nil );
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- If the user has passed in settings that override the defaults
  -- (the argList), then process that now.
  if argList ~= nil then
    adjustLListMap( ldtMap, argList ); -- ldtMap here, not ldtList
    topRec[ldtBinName] = ldtList; -- Update after adjustment
  end

  GP=F and trace("[DEBUG]<%s:%s> LLIST after Init(%s)",
   MOD, meth, ldtSummaryString( ldtList ) );

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

  local ldtList;
  local propMap;
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
    ldtList = initializeLList( topRec, ldtBinName, nil, nil );
    propMap = ldtList[1];
    ldtMap  = ldtList[2];
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLListMap( ldtMap, createSpec ); -- Map, not list, used here
    end
    topRec[ldtBinName] = ldtMap;
  else
    -- all there, just use it
    ldtList = topRec[ ldtBinName ];
    propMap = ldtList[1];
    ldtMap  = ldtList[2];
  end
  -- Note: We'll do the aerospike:create() at the end of this function,
  -- if needed.

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to turn our single list into a tree.
  local totalCount = ldtMap[R_TotalCount];
  if ldtMap[R_StoreState] == SS_COMPACT and
    totalCount >= ldtMap[R_Threshold]
  then
    convertList( topRec, ldtBinName, ldtMap ); -- Map, not list, used here
  end
 
  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, ldtList, newValue, true );

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

  local rc = 0;
  -- Define our return list
  local resultList = list();
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  local ldtList = topRec[ldtBinName];
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- If our state is "compact", do a simple list search, otherwise do a
  -- full tree search.
  if( ldtMap[R_StoreState] == SS_COMPACT ) then 
    GP=F and trace("[DEBUG]<%s:%s> Searching Compact List", MOD, meth );
    local objList = ldtMap[R_CompactList];
    rc = scanList(resultList,ldtList,objList,searchValue,FV_SCAN,func,fargs);
  else
    GP=F and trace("[DEBUG]<%s:%s> Searching Tree", MOD, meth );
    local searchPath = createSearchPath();
    rc = treeSearch( topRec, searchPath, ldtList, searchValue );
    if( rc == 0 ) then
      rc = treeScan(resultList, topRec, searchPath, ldtList, searchValue,
                    func, fargs );
    end
  end -- tree search

  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
  MOD, meth, tostring(result));
  
  -- TODO : NOTE :: Need to Pass a RESULT OBJECT HERE -- not just a status
  -- or result list.
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

  GP=F and trace("[ENTER]<%s:%s>ldtBinName(%s) deleteValue(%s)",
      MOD, meth, tostring(ldtBinName), tostring(deleteValue));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Call local delete to do the real work.
  local rc  = localDelete( topRec, ldtBinName, deleteValue );

  -- Validate results -- if anything bad happened, then the record
  -- probably did not change -- we don't need to udpate.
  if( rc == 0 ) then
    -- All done, store the record
    local rc = -99; -- Use Odd starting Num: so that we know it got changed
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

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

  -- Extract the property map and control map from the ldt bin list.
  local ldtList = topRec[ ldtBinName ];
  local propMap = ldtList[1];
  local itemCount = propMap[PM_ItemCount];

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
