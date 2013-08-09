-- Large Ordered List (llist.lua)
-- Last Update August 09,  2013: tjl
--
-- Keep this MOD value in sync with version above
local MOD = "llist_2013_08_09.d"; -- module name used for tracing.  

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.1;

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).  We may also use "F" as a general guard for larger
-- print debug blocks -- as well as the individual trace/info lines.
-- ======================================================================
local GP=true;
-- local F=true; -- Set F (flag) to true to turn ON global print
local F=false; -- Set F (flag) to true to turn ON global print
-- local E=true; -- Set F (flag) to true to turn ON Enter/Exit print
local E=false; -- Set F (flag) to true to turn ON Enter/Exit print
-- local F=false; -- Set F (flag) to false to turn OFF global print

-- TODO (Major Feature Items:  (N) Now, (L) Later
-- (N) Switch all Lua External functions to return two-part values, which
--     Need to match what the new C API expects (when Chris returns).
-- (N) Handle Duplicates (search, Scan, Delete)
-- (L) Vector Operations for Insert, Search, Scan, Delete
--     ==> A LIST of Operations to perform, along with a LIST of RESULT
--         to return.
-- (L) Change the SubRec Context to close READONLY pages when we're done
--     with them -- and keep open ONLY the dirty pages.  So, we have to mark
--     dirty pages in the SRC.  We could manage the SRC like a buffer pool
--     that closes oldest READONLY pages when it needs space.
-- (L) Build the tree from the list, using "buildTree()" method, rather
--     than individual inserts.  Sorted list is broken into leaves, which
--     become tree leaves.  Allocate parents as necessary.  Build bottom
--     up.
-- TODO (Minor Design changes, Adjustments and Fixes)
-- TODO (Testing)
-- (*) Fix "leaf Count" in the ldt map
-- (*) Test/validate Simple delete
-- (*) Test/validate Simple scan
-- (*) Complex Insert
-- (*) Complex Search
-- (*) Complex delete
-- (*) Tree Delete (Remove)
-- (*) Switch CompactList to Sorted List (like the leaf list)
-- (*) Switch CompactList routines to use the "Leaf" List routines
--     for search, insert, delete and scan.
--     Search: Return success and position
--     Insert: Search, plus listInsert
--     Delete: Search, plus listDelete
--     Scan:   Search, plus listScan
-- (*) Test that Complex Type and KeyFunction is defined on create,
--     otherwise error.  Take no default action.
--
-- DONE LIST
-- (*) Initialize Maps for Root, Nodes, Leaves
-- (*) Create Search Function
-- (*) Simple Insert (Root plus Leaf Insert)
-- (*) Complex Node Split Insert (Root and Inner Nodes)
-- (*) Simple Delete
-- (*) Simple Scan
-- ======================================================================
-- FORWARD Function DECLARATIONS
-- ======================================================================
-- We have some circular (recursive) function calls, so to make that work
-- we have to predeclare some of them here (they look like local variables)
-- and then later assign the function body to them.
-- ======================================================================
local insertParentNode;

-- ++==================++
-- || External Modules ||
-- ++==================++
-- Set up our "outside" links.
-- Get addressability to the Function Table: Used for compress/transform,
-- keyExtract, Filters, etc. 
local functionTable = require('UdfFunctionTable');

-- When we're ready, we'll move all of our common routines into ldt_common,
-- which will help code maintenance and management.
-- local LDTC = require('ldt_common');

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt_errors');

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
-- (*) llist_scan:   Scan the entire tree
-- (*) llist_remove: Remove the entire LDT from the record and remove bin.
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
-- Aerospike Server Functions:
-- ======================================================================
-- Aerospike Record Functions:
-- status = aerospike:create( topRec )
-- status = aerospike:update( topRec )
-- status = aerospike:remove( rec ) (not currently used)
--
-- Aerospike SubRecord Functions:
-- newRec = aerospike:create_subrec( topRec )
-- rec    = aerospike:open_subrec( topRec, digestString )
-- status = aerospike:update_subrec( childRec )
-- status = aerospike:close_subrec( childRec )
-- status = aerospike:remove_subrec( subRec ) 
--
-- Record Functions:
-- digest = record.digest( childRec )
-- status = record.set_type( topRec, recType )
-- status = record.set_flags( topRec, binName, binFlags )
--
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

-- Values used in Compare (CR = Compare Results)
local CR_LESS_THAN      = -1;
local CR_EQUAL          =  0;
local CR_GREATER_THAN   =  1;
local CR_ERROR          = -2;
local CR_INTERNAL_ERROR = -3;

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

-- Scan Status:  Do we keep scanning, or stop?
local SCAN_ERROR        = -1;  -- Error during Scanning
local SCAN_DONE         =  0;  -- Done scanning
local SCAN_CONINTUE     =  1;  -- Keep Scanning

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
-- come back to bite me.
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (RT_LEAF NOT used in this context)
-- (2) As a TYPE in our own propMap[PM_RecType] field: CDIR *IS* used here.
local RT_REG  = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT  = 1; -- 0x1: Top Record (contains an LDT)
local RT_NODE = 2; -- 0x2: Regular Sub Record (Node, Leaf)
local RT_SUB  = 2; -- 0x2: Regular Sub Record (Node, Leaf)::Used for set_type
local RT_LEAF = 3; -- xxx: Leaf Nodes:: Not used for set_type() 
local RT_ESR  = 4; -- 0x4: Existence Sub Record

-- We maintain a pool, or "context", of subrecords that are open.  That allows
-- us to look up subrecs and get the open reference, rather than bothering
-- the lower level infrastructure.  There's also a limit to the number
-- of open subrecs.
local G_OPEN_SR_LIMIT = 20;

-- ------------------------------------------------------------------------
-- Control Map Names: for Property Maps and Control Maps
-- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values -- within any given map.  They do NOT have to be unique across
-- the maps (and there's no need -- they serve different purposes).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local RPM_LdtCount             = 'C';  -- Number of LDTs in this rec
local RPM_VInfo                = 'V';  -- Partition Version Info
local RPM_Magic                = 'Z';  -- Special Sauce
local RPM_SelfDigest           = 'D';  -- Digest of this record

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local PM_ItemCount             = 'I'; -- (Top): Count of all items in LDT
local PM_Version               = 'V'; -- (Top): Code Version
local PM_SubRecCount           = 'S'; -- (Top): # of subrecs in the LDT
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_CreateTime            = 'C'; -- (All): Creation time of this rec
local PM_EsrDigest             = 'E'; -- (All): Digest of ESR
local PM_RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
local PM_LogInfo               = 'L'; -- (All): Log Info (currently unused)
local PM_ParentDigest          = 'P'; -- (Subrec): Digest of TopRec
local PM_SelfDigest            = 'D'; -- (Subrec): Digest of THIS Record

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Leaf and Node Fields (There is some overlap between nodes and leaves)
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LF_ListEntryCount       = 'L';-- # current list entries used
local LF_ListEntryTotal       = 'T';-- # total list entries allocated
local LF_ByteEntryCount       = 'B';-- # current bytes used
local LF_PrevPage             = 'P';-- Digest of Previous (left) Leaf Page
local LF_NextPage             = 'N';-- Digest of Next (right) Leaf Page

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
local R_KeyFunction         = 'F';-- Function to compute Key from Object
-- Key and Object Sizes, when using fixed length (byte array stuff)
local R_KeyByteSize         = 'B';-- Fixed Size (in bytes) of Key
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
-- F:R_KeyFunction            f:                        5:
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
local PackageStandardList        = "StandardList";
local PackageTestModeObject      = "TestModeObject";
local PackageTestModeObjectDup   = "TestModeObjectDup";
local PackageTestModeList        = "TestModeList";
local PackageTestModeBinary      = "TestModeBinary";
local PackageTestModeNumber      = "TestModeNumber";
local PackageTestModeNumberDup   = "TestModeNumberDup";
local PackageDebugModeObjectDup  = "DebugModeObjectDup";
local PackageDebugModeObject     = "DebugModeObject";
local PackageDebugModeList       = "DebugModeList";
local PackageDebugModeBinary     = "DebugModeBinary";
local PackageDebugModeNumber     = "DebugModeNumber";
local PackageDebugModeNumberDup  = "DebugModeNumberDup";
local PackageProdListValBinStore = "ProdListValBinStore";

-- set up our "outside" links
-- local  CRC32 = require('CRC32'); -- Used by LSET, LMAP
local functionTable = require('UdfFunctionTable');
-- local LDTC = require('ldt_common');

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
  resultMap.PropSubRecCount   = propMap[PM_SubRecCount];
  resultMap.PropVersion       = propMap[PM_Version];
  resultMap.PropLdtType       = propMap[PM_LdtType];
  resultMap.PropEsrDigest     = propMap[PM_EsrDigest];
  resultMap.PropMagic         = propMap[PM_Magic];
  resultMap.PropCreateTime    = propMap[PM_CreateTime];


  -- General Tree Settings
  resultMap.StoreMode         = ldtMap[R_StoreMode];
  resultMap.StoreState        = ldtMap[R_StoreState];
  resultMap.TreeLevel         = ldtMap[R_TreeLevel];
  resultMap.LeafCount         = ldtMap[R_LeafCount];
  resultMap.NodeCount         = ldtMap[R_NodeCount];
  resultMap.KeyType           = ldtMap[R_KeyType];
  resultMap.TransFunc         = ldtMap[R_TransFunc];
  resultMap.UnTransFunc       = ldtMap[R_UnTransFunc];
  resultMap.KeyFunction       = ldtMap[R_KeyFunction];

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

-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- TODO: Move this to LDT_COMMON (7/21/2013)
-- ======================================================================
local function setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if( topRec[REC_LDT_CTRL_BIN] == nil ) then
    GP=F and trace("[DEBUG]<%s:%s>Creating Record LDT Map", MOD, meth );

    -- If this record doesn't even exist yet -- then create it now.
    -- Otherwise, things break.
    if( not aerospike:exists( topRec ) ) then
      GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
      rc = aerospike:create( topRec );
    end

    record.set_type( topRec, RT_LDT );
    recPropMap = map();
    -- vinfo will be a 5 byte value, but it will be easier for us to store
    -- 6 bytes -- and just leave the high order one at zero.
    -- Initialize the VINFO value to all zeros.
    -- local vinfo = bytes(6);
    -- bytes.put_int16(vinfo, 1, 0 );
    -- bytes.put_int16(vinfo, 3, 0 );
    -- bytes.put_int16(vinfo, 5, 0 );
    -- Change this to use a number, rather than bytes.
    local vinfo = 0;
    recPropMap[RPM_VInfo] = vinfo; -- to be replaced later - on the server side.
    recPropMap[RPM_LdtCount] = 1; -- this is the first one.
    recPropMap[RPM_Magic] = MAGIC;
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM_LdtCount];
    recPropMap[RPM_LdtCount] = ldtCount + 1;
    GP=F and trace("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1 );
  end
  topRec[REC_LDT_CTRL_BIN] = recPropMap;

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- setLdtRecordType()

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
-- ldtList = topRec[ldtBinName]
-- local propMap = ldtList[1];
-- local ldtMap  = ldtList[2];
-- ======================================================================
local function
initializeLList( topRec, ldtBinName )
  local meth = "initializeLList()";
  GP=E and trace("[ENTER]<%s:%s>:: ldtBinName(%s)",
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
  propMap[PM_SubRecCount] = 0; -- No Subrecs yet
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LLIST; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]    = 0; -- not set yet.
  propMap[PM_CreateTime] = aerospike:get_current_time();
  propMap[PM_SelfDigest]  = record.digest( topRec );

  -- General Tree Settings
  ldtMap[R_TotalCount] = 0;    -- A count of all "slots" used in LLIST
  ldtMap[R_LeafCount] = 0;     -- A count of all Leaf Nodes
  ldtMap[R_NodeCount] = 0;     -- A count of all Nodes (incl leaves, excl root)
  ldtMap[R_StoreMode] = SM_LIST; -- SM_LIST or SM_BINARY (applies to all nodes)
  ldtMap[R_TreeLevel] = 1;     -- Start off Lvl 1: Root ONLY. Leaves Come l8tr
  ldtMap[R_KeyType]   = KT_ATOMIC;-- atomic or complex
  ldtMap[R_KeyUnique] = false; -- Keys are NOT unique by default.
  ldtMap[R_TransFunc] = nil; -- (set later) transform Func (user to storage)
  ldtMap[R_UnTransFunc] = nil; -- (set later) Un-transform (storage to user)
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

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method also sets this toprec as an LDT type record.
  setLdtRecordType( topRec );
  
  -- Set the BIN Flag type to show that this is an LDT Bin, with all of
  -- the special priviledges and restrictions that go with it.
  GP=F and trace("[DEBUG]:<%s:%s>About to call record.set_flags(Bin(%s)F(%s))",
    MOD, meth, ldtBinName, tostring(BF_LDT_BIN) );

  record.set_flags( topRec, ldtBinName, BF_LDT_BIN );

  GP=F and trace("[DEBUG]: <%s:%s> Back from calling record.set_flags()",
  MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s> : CTRL Map after Init(%s)",
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
  ldtMap[R_Threshold] = DEFAULT_THRESHOLD; -- Rehash after this many inserts
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 100;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 100;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

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
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
  ldtMap[R_Threshold] = 20; -- Change to TREE Ops after this many inserts
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Unique values only.
 
  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 20;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 20;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

  return 0;
end -- packageTestModeNumber()

-- ======================================================================
-- Package = "TestModeNumberDup"
-- ======================================================================
local function packageTestModeNumberDup( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
  ldtMap[R_Threshold] = 20; -- Change to TREE Ops after this many inserts
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  ldtMap[R_KeyUnique] = false; -- allow Duplicates
 
  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 20;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 20;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

  return 0;
end -- packageTestModeNumberDup()

-- ======================================================================
-- Package = "TestModeObjectDup"
-- ======================================================================
local function packageTestModeObjectDup( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
  ldtMap[R_Threshold] = 20; -- Change to TREE Ops after this many inserts
  -- Use the special function that simply returns the value held in
  -- the object's map field "key".
  ldtMap[R_KeyFunction] = "keyExtract"; -- Special Attention Required.
  ldtMap[R_KeyUnique] = false; -- allow Duplicates
 
  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 20;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 20;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

  return 0;
end -- packageTestModeObjectDup()


-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
local function packageTestModeObject( ldtMap )
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
  ldtMap[R_Threshold] = 10; -- Change to TREE Ops after this many inserts
  -- Use the special function that simply returns the value held in
  -- the object's map field "key".
  ldtMap[R_KeyFunction] = "keyExtract"; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Assume Unique Objects
 
  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 100;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 100;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

  return 0;
end -- packageTestModeObject()

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
  -- ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 2; -- Change to TREE Operations after this many inserts
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Assume Unique Objects
 
  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 100;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 100;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

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
  -- ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 2; -- Change to TREE Mode after this many ops.
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
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
  -- ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 100; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  return 0;
  
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
-- where we sort them based on a map field called "key".
-- ======================================================================
local function packageDebugModeObject( ldtMap )
  local meth = "packageDebugModeObject()";
  
  GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
      MOD, meth , tostring(ldtMap));

  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Atomic Keys
  ldtMap[R_Threshold] = 2; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = "keyExtract"; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Just Unique keys for now.

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 4;  -- Max # of items (key+digest)

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 4;  -- Max # of items

  GP=E and trace("[EXIT]<%s:%s> : ldtMap(%s)",
      MOD, meth , tostring(ldtMap));

  return 0;

end -- packageDebugModeObject()


-- ======================================================================
-- Package = "DebugModeObjectDup"
-- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
-- where we sort them based on a map field called "key".
-- ASSUME that we will support DUPLICATES.
-- ======================================================================
local function packageDebugModeObjectDup( ldtMap )
  local meth = "packageDebugModeObjectDup()";
  
  GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
      MOD, meth , tostring(ldtMap));

  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_COMPLEX; -- Atomic Keys
  ldtMap[R_Threshold] = 2; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = "keyExtract"; -- Special Attention Required.
  ldtMap[R_KeyUnique] = false; -- Assume there will be Duplicates

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 4;  -- Max # of items (key+digest)

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 4;  -- Max # of items

  return 0;

end -- packageDebugModeObjectDup()


-- ======================================================================
-- Package = "DebugModeList"
-- Test the LLIST with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( ldtMap )
  local meth = "packageDebugModeList()";
  
  GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
      MOD, meth , tostring(ldtMap));

  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Atomic Keys
  ldtMap[R_Threshold] = 10; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Just Unique keys for now.

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 10; -- Length of Key List (page list is KL + 1)
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 10;  -- Max # of items (key+digest)

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 10;  -- Max # of items

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
  -- ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 4; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  return 0;

end -- packageDebugModeBinary( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( ldtMap )
  local meth = "packageDebugModeNumber()";
  GP=E and trace("[ENTER]<%s:%s>:: LdtMap(%s)",
    MOD, meth, tostring(ldtMap) );
  
  -- General Parameters
  ldtMap[R_Transform] = nil;
  ldtMap[R_UnTransform] = nil;
  ldtMap[R_KeyCompare] = nil;
  ldtMap[R_StoreState] = SS_COMPACT; -- start in "compact mode"
  ldtMap[R_StoreMode] = SM_LIST; -- Use List Mode
  ldtMap[R_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
  ldtMap[R_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
  -- ldtMap[R_BinName] = ldtBinName;
  ldtMap[R_Threshold] = 4; -- Rehash after this many have been inserted
  ldtMap[R_KeyFunction] = nil; -- Special Attention Required.
  ldtMap[R_KeyUnique] = true; -- Just Unique keys for now.

  -- Top Node Tree Root Directory
  ldtMap[R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
  ldtMap[R_RootByteCountMax] = 0; -- Max bytes for key space in the root
  
  -- LLIST Inner Node Settings
  ldtMap[R_NodeListMax] = 4;  -- Max # of items (key+digest)
  ldtMap[R_NodeByteCountMax] = 0; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap[R_LeafListMax] = 4;  -- Max # of items
  ldtMap[R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

  GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
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
  GP=E and trace("[ENTER]<%s:%s>:: LListMap(%s)::\n ArgListMap(%s)",
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
      elseif value == PackageTestModeObject then
          packageTestModeObject( ldtMap );
      elseif value == PackageTestModeObjectDup then
          packageTestModeObjectDup( ldtMap );
      elseif value == PackageTestModeList then
          packageTestModeList( ldtMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( ldtMap );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( ldtMap );
      elseif value == PackageTestModeNumberDup then
          packageTestModeNumberDup( ldtMap );
      elseif value == PackageProdListValBinStore then
          packageProdListValBinStore( ldtMap );
      elseif value == PackageDebugModeObjectDup then
          packageDebugModeObjectDup( ldtMap );
      elseif value == PackageDebugModeObject then
          packageDebugModeObject( ldtMap );
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

  GP=E and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));
      
  return ldtMap
end -- adjustLListMap


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree Leaf Nodes have four bins:
-- Each LDT Data Record (LDR) holds a small amount of control information
-- and a list.  A LDR will have four bins:
-- (1) A Property Map Bin (the same for all LDT subrecords)
-- (2) The Control Bin (a Map with the various control data)
-- (3) The Data List Bin -- where we hold Object "list entries"
-- (4) The Binary Bin -- (Optional) where we hold compacted binary entries
--    (just the as bytes values)
--
-- Records used for B+ Tree Inner Nodes have five bins:
-- (1) A Property Map Bin (the same for all LDT subrecords)
-- (2) The Control Bin (a Map with the various control data)
-- (3) The key List Bin -- where we hold Key "list entries"
-- (4) The Digest List Bin -- where we hold the digests
-- (5) The Binary Bin -- (Optional) where we hold compacted binary entries
--    (just the as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 3) or BINARY MODE (bin 5)
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
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local topPropMap = ldtList[1];
  local ldtMap     = ldtList[2];

  -- Set up our new property and control map for this node
  local nodePropMap = map();
  local nodeMap = map();

  nodePropMap[PM_Magic] = MAGIC;
  nodePropMap[PM_EsrDigest] = topPropMap[PM_EsrDigest];
  nodePropMap[PM_RecType] = RT_NODE;
  -- nodePropMap[PM_ParentDigest] = topPropMap[PM_SelfDigest];
  nodePropMap[PM_ParentDigest] = record.digest( topRec );
  nodePropMap[PM_SelfDigest] = record.digest( nodeRec );

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

  -- We must tell the system what type of record this is (sub-record)
  record.set_type( nodeRec, RT_SUB );

  aerospike:update_subrec( nodeRec );

  -- If we had BINARY MODE working for inner nodes, we would initialize
  -- the Key BYTE ARRAY here.  However, the real savings would be in the
  -- leaves, so it may not be much of an advantage to use binary mode in nodes.

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
-- There are potentially FOUR bins in an Interior Tree Node Record:
-- (0) nodeRec[SUBREC_PROP_BIN]: The Property Map
-- (1) nodeRec[LSR_CTRL_BIN]:   The control Map (defined here)
-- (2) nodeRec[LSR_LIST_BIN]:   The Data Entry List (when in list mode)
-- (3) nodeRec[LSR_BINARY_BIN]: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only four fields.
-- Either Bins 0,1,2,4 or Bins 0,1,3,5.
-- Parms:
-- (*) topRec
-- (*) ldtList
-- (*) leafRec
-- (*) firstValue
-- (*) pd: previous (left) Leaf Digest (or 0, if not there)
-- (*) nd: next (right) Leaf Digest (or 0, if not there)
-- ======================================================================
local function initializeLeaf(topRec, ldtList, leafRec, firstValue, pd, nd )
  local meth = "initializeLeaf()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>:1st Val(%s)", MOD, meth, tostring(firstValue));

  local topDigest = record.digest( topRec );
  local leafDigest = record.digest( leafRec );
  
  -- Extract the property map and control map from the ldt bin list.
  local topPropMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Set up the Property Map
  leafPropMap = map();
  leafPropMap[PM_Magic] = MAGIC;
  leafPropMap[PM_EsrDigest] = topPropMap[PM_EsrDigest]; 
  leafPropMap[PM_RecType] = RT_LEAF;
  leafPropMap[PM_ParentDigest] = topDigest;
  leafPropMap[PM_SelfDigest] = leafDigest;
  leafPropMap[PM_CreateTime] = topPropMap[PM_CreateTime];

  leafMap = map();
  if( ldtMap[R_StoreMode] == SM_LIST ) then
    -- List Mode
    GP=F and trace("[DEBUG]: <%s:%s> Initialize in LIST mode", MOD, meth );
    leafMap[LF_ByteEntryCount] = 0;
    -- If we have an initial value, then enter that in our new object list.
    -- Otherwise, create an empty list.
    local objectList = list();
    if( firstValue ~= nil ) then
      list.append( objectList, firstValue );
      leafMap[LF_ListEntryCount] = 1;
      leafMap[LF_ListEntryTotal] = 1;
    else
      leafMap[LF_ListEntryCount] = 0;
      leafMap[LF_ListEntryTotal] = 0;
    end
    leafRec[LSR_LIST_BIN] = objectList;
  else
    -- Binary Mode
    GP=F and trace("[DEBUG]: <%s:%s> Initialize in BINARY mode", MOD, meth );
    warn("[WARNING!!!]<%s:%s>Not ready for BINARY MODE YET!!!!", MOD, meth );
    leafMap[LF_ListEntryTotal] = 0;
    leafMap[LF_ListEntryCount] = 0;
    leafMap[LF_ByteEntryCount] = startCount;
  end

  -- Take our new structures and put them in the leaf record.
  leafRec[SUBREC_PROP_BIN] = leafPropMap;
  leafRec[LSR_CTRL_BIN] = leafMap;
  -- We must tell the system what type of record this is (sub-record)
  record.set_type( leafRec, RT_SUB );

  aerospike:update_subrec( leafRec );
  -- Note that the caller will write out the record, since there will
  -- possibly be more to do (like add data values to the object list).
  GP=F and trace("[DEBUG]<%s:%s> TopRec Digest(%s) Leaf Digest(%s))",
    MOD, meth, tostring(topDigest), tostring(leafDigest));

  GP=F and trace("[DEBUG]<%s:%s> LeafPropMap(%s) Leaf Map(%s)",
    MOD, meth, tostring(leafPropMap), tostring(leafMap));

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- initializeLeaf()

-- ======================================================================
-- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> --
--           Large Ordered List (LLIST) Utility Functions
-- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> --
-- ======================================================================
-- These are all local functions to this module and serve various
-- utility and assistance functions.
-- ======================================================================

-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getLeafMap( leafRec )
  -- local meth = "getLeafMap()";
  -- GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );
  return leafRec[LSR_CTRL_BIN]; -- this should be a map.
end -- getLeafMap


-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getNodeMap( nodeRec )
  -- local meth = "getNodeMap()";
  -- GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );
  return nodeRec[NSR_CTRL_BIN]; -- this should be a map.
end -- getNodeMap

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( binName )
  local meth = "validateBinName()";

  GP=E and trace("[ENTER]<%s:%s> validate Bin Name(%s)",
    MOD, meth, tostring(binName));

  if binName == nil  then
    WARN("[ERROR]<%s:%s> Bin Name is NULL", MOD, meth );
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( binName ) ~= "string"  then
    WARN("[ERROR]<%s:%s> Bin Name is Not a String: Type(%s)", MOD, meth,
      tostring( type(binName) ));
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( binName ) > 14 then
    WARN("[ERROR]<%s:%s> Bin Name Too Long::Exceeds 14 characters", MOD, meth);
    error( ldte.ERR_BIN_NAME_TOO_LONG );
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
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> BinName(%s) ME(%s)",
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
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end

    -- Control Bin Must Exist
    if( topRec[ldtBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LDT BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    if ( propMap[PM_Magic] ~= MAGIC ) then
      GP=E and warn("[ERROR EXIT]:<%s:%s>LDT BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( ldtBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[ldtBinName] ~= nil then
      local ldtList = topRec[ldtBinName];
      local propMap = ldtList[1];
      local ldtMap  = ldtList[2];
      if ( propMap[PM_Magic] ~= MAGIC ) then
        GP=E and warn("[ERROR EXIT]:<%s:%s> LDT BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( ldtBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
    end -- if worth checking
  end -- else for must exist

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
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
-- printRoot( topRec, ldtList )
-- ======================================================================
-- Dump the Root contents for Debugging/Tracing purposes
-- ======================================================================
local function printRoot( topRec, ldtList )
  -- Extract the property map and control map from the ldt bin list.
  local pMap       = ldtList[1];
  local cMap       = ldtList[2];
  local keyList    = cMap[R_RootKeyList];
  local digestList = cMap[R_RootDigestList];
  local binName    = pMap[PM_BinName];
  -- if( F == true ) then
    trace("\n RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
    trace("\n ROOT::Bin(%s)", binName );
    trace("\n ROOT::PMAP(%s)", tostring( pMap ) );
    trace("\n ROOT::CMAP(%s)", tostring( cMap ) );
    trace("\n ROOT::KeyList(%s)", tostring( keyList ) );
    trace("\n ROOT::DigestList(%s)", tostring( digestList ) );
  -- end
end -- printRoot()

-- ======================================================================
-- printNode( topRec, ldtList )
-- ======================================================================
-- Dump the Node contents for Debugging/Tracing purposes
-- ======================================================================
local function printNode( nodeRec )
  local pMap        = nodeRec[SUBREC_PROP_BIN];
  local cMap        = nodeRec[NSR_CTRL_BIN];
  local keyList     = nodeRec[NSR_KEY_LIST_BIN];
  local digestList  = nodeRec[NSR_DIGEST_BIN];
  -- if( F == true ) then
    trace("\n NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN");
    trace("\n NODE::PMAP(%s)", tostring( pMap ) );
    trace("\n NODE::CMAP(%s)", tostring( cMap ) );
    trace("\n NODE::KeyList(%s)", tostring( keyList ) );
    trace("\n NODE::DigestList(%s)", tostring( digestList ) );
  -- end
end -- printNode()

-- ======================================================================
-- printLeaf( topRec, ldtList )
-- ======================================================================
-- Dump the Leaf contents for Debugging/Tracing purposes
-- ======================================================================
local function printLeaf( leafRec )
  local pMap     = leafRec[SUBREC_PROP_BIN];
  local cMap     = leafRec[LSR_CTRL_BIN];
  local objList  = leafRec[LSR_LIST_BIN];
  -- if( F == true ) then
    trace("\n LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
    trace("\n LEAF::PMAP(%s)", tostring( pMap ) );
    trace("\n LEAF::CMAP(%s)", tostring( cMap ) );
    trace("\n LEAF::ObjectList(%s)", tostring( objList ) );
  -- end
end -- printLeaf()

-- ======================================================================
-- rootNodeSummary( ldtList )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function rootNodeSummary( ldtList )
  local resultMap = ldtList;

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( ldtSummary( ldtList )  );
end -- rootNodeSummary()

-- ======================================================================
-- nodeSummary( nodeRec )
-- nodeSummaryString( nodeRec )
-- ======================================================================
-- Print out interesting stats about this Interior B+ Tree Node
-- ======================================================================
local function nodeSummary( nodeRec )
  local meth = "nodeSummary()";
  local resultMap = map();
  local propMap  = nodeRec[SUBREC_PROP_BIN];
  local nodeMap  = nodeRec[NSR_CTRL_BIN];
  local keyList = nodeRec[NSR_KEY_LIST_BIN];
  local digestList = nodeRec[NSR_DIGEST_BIN];

  -- General Properties (the Properties Bin)
  resultMap.SUMMARY           = "NODE Summary";
  resultMap.PropMagic         = propMap[PM_Magic];
  resultMap.PropCreateTime    = propMap[PM_CreateTime];
  resultMap.PropEsrDigest     = propMap[PM_EsrDigest];
  resultMap.PropRecordType    = propMap[PM_RecType];
  resultMap.PropParentDigest  = propMap[PM_ParentDigest];
  
  -- Node Control Map
  resultMap.ListEntryCount = nodeMap[ND_ListEntryCount];
  resultMap.ListEntryTotal = nodeMap[ND_ListEntryTotal];

  -- Node Contents (Object List)
  resultMap.KEY_LIST              = keyList;
  resultMap.DIGEST_LIST           = digestList;

  return resultMap;
end -- nodeSummary()

local function nodeSummaryString( nodeRec )
  return tostring( nodeSummary( nodeRec ) );
end -- nodeSummaryString()

-- ======================================================================
-- leafSummary( leafRec )
-- leafSummaryString( leafRec )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Leaf (Data) node
-- ======================================================================
local function leafSummary( leafRec )
  if( leafRec == nil ) then
    return "NIL Leaf Record";
  end

  local resultMap = map();
  local propMap   = leafRec[SUBREC_PROP_BIN];
  local leafMap   = leafRec[LSR_CTRL_BIN];
  local leafList  = leafRec[LSR_LIST_BIN];

  -- General Properties (the Properties Bin)
  resultMap.SUMMARY           = "LEAF Summary";
  resultMap.PropMagic         = propMap[PM_Magic];
  resultMap.PropCreateTime    = propMap[PM_CreateTime];
  resultMap.PropEsrDigest     = propMap[PM_EsrDigest];
  resultMap.PropSelfDigest    = propMap[PM_SelfDigest];
  resultMap.PropRecordType    = propMap[PM_RecType];
  resultMap.PropParentDigest  = propMap[PM_ParentDigest];

  trace("[LEAF PROPS]: %s", tostring(resultMap));
  
  -- Leaf Control Map
  resultMap.LF_ListEntryCount = leafMap[LF_ListEntryCount];
  resultMap.LF_ListEntryTotal = leafMap[LF_ListEntryTotal];
  resultMap.LF_PrevPage       = leafMap[LF_PrevPage];
  resultMap.LF_NextPage       = leafMap[LF_NextPage];

  -- Leaf Contents (Object List)
  resultMap.LIST              = leafList;

  return resultMap;
end -- leafSummary()

local function leafSummaryString( leafRec )
  return tostring( leafSummary( leafRec ) );
end

-- ======================================================================
-- ======================================================================
local function showRecSummary( nodeRec, propMap )
  local meth = "showRecSummary()";
  -- Debug/Tracing to see what we're putting in the SubRec Context
  -- if( F == true ) then
  if( propMap == nil ) then
    warn("[ERROR]<%s:%s>: propMap value is NIL", MOD, meth );
    error( ldte.ERR_SUBREC_DAMAGED );
  end
    GP=F and trace("\n[SUBREC DEBUG]:: SRC Contents \n");
    local recType = propMap[PM_RecType];
    if( recType == RT_LEAF ) then
      GP=F and trace("\n[Leaf Record Summary] %s\n",leafSummaryString(nodeRec));
    elseif( recType == RT_NODE ) then
      GP=F and trace("\n[Node Record Summary] %s\n",nodeSummaryString(nodeRec));
    else
      GP=F and trace("\n[OTHER Record TYPE] (%s)\n", tostring( recType ));
    end
  -- end
end -- showRecSummary()


-- =============================
-- Begin SubRecord Function Area
-- =============================
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, like insert(), search(), delete(),
-- will employ the "subrecContext" object, which will hold all of the
-- subrecords that were opened during processing.  Note that with
-- B+ Trees, operations like insert() can potentially involve many subrec
-- operations -- and can also potentially revisit pages.  In addition,
-- we employ a "compact list", which gets converted into tree inserts when
-- we cross a threshold value, so that will involve MANY subrec "re-opens"
-- that would confuse the underlying infrastructure.
--
-- SubRecContext Design:
-- The key will be the DigestString, and the value will be the subRec
-- pointer.  At the end of an outer call, we will iterate thru the subrec
-- context and close all open subrecords.  Note that we may also need
-- to mark them dirty -- but for now we'll update them in place (as needed),
-- but we won't close them until the end.
-- ======================================================================
local function createSubrecContext()
  local meth = "createSubrecContext()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  local src = map();
  src.ItemCount = 0;

  GP=E and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(src));
  return src;
end -- createSubrecContext()

-- ======================================================================
-- Given an already opened subrec (probably one that was recently created),
-- add it to the subrec context.
-- ======================================================================
local function addSubrecToContext( src, nodeRec )
  local meth = "addSubrecContext()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  if( src == nil ) then
    warn("[ERROR]<%s:%s> SubRec Pool is nil", MOD, meth );
    error( ldte.ERR_SUBREC_POOL_DAMAGED );
  end

  local digest = record.digest( nodeRec );
  local digestString = tostring( digest );
  src[digestString] = nodeRec;

  local itemCount = src.ItemCount;
  src.ItemCount = itemCount + 1;

--  trace("\n[ADD SUBREC]<%s:%s> SRC(%s) nodeRec(%s) Digest(%s) IC(%d)\n",
--  MOD, meth, tostring(src), tostring(nodeRec), digestString, src.ItemCount);

  -- Debug/Tracing to see what we're putting in the SubRec Context
--  if( F == true ) then
--    local propMap = nodeRec[SUBREC_PROP_BIN];
--    showRecSummary( nodeRec, propMap );
--  end

  GP=E and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(src));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- ======================================================================
local function openSubrec( src, topRec, digestString )
  local meth = "openSubrec()";
  GP=E and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
    MOD, meth, tostring(topRec), digestString, tostring(src));

  -- We have a global limit on the number of subrecs that we can have
  -- open at a time.  If we're at (or above) the limit, then we must
  -- exit with an error (better here than in the subrec code).
  local itemCount = src.ItemCount;

  local rec = src[digestString];
  if( rec == nil ) then
    if( itemCount >= G_OPEN_SR_LIMIT ) then
      warn("[ERROR]<%s:%s> SRC Count(%d) Exceeded Limit(%d)", MOD, meth,
        itemCount, G_OPEN_SR_LIMIT );
      error( ldte.ERR_TOO_MANY_OPEN_SUBRECS );
    end

    src.ItemCount = itemCount + 1;
    GP=F and trace("[OPEN SUBREC]<%s:%s>SRC.ItemCount(%d) TR(%s) DigStr(%s)",
      MOD, meth, src.ItemCount, tostring(topRec), digestString );
    rec = aerospike:open_subrec( topRec, digestString );
    GP=F and trace("[OPEN SUBREC RESULTS]<%s:%s>(%s)",MOD,meth,tostring(rec));
    if( rec == nil ) then
      warn("[ERROR]<%s:%s> Subrec Open Failure: Digest(%s)", MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
  else
    GP=F and trace("[FOUND REC]: <%s:%s> : Rec(%s)", MOD, meth, tostring(rec));
  end

  -- Debug/Tracing to see what is in the SubRec Context
  GP=F and showRecSummary( rec, rec[SUBREC_PROP_BIN] );

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
    MOD, meth, tostring(rec), digestString );
  return rec;
end -- openSubrec()

-- ======================================================================
-- ======================================================================
local function closeAllSubrecs( src )
  local meth = "closeAllSubrecs()";
  GP=E and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(src));

  -- Iterate thru the SubRecContext and close all subrecords.
  local digestString;
  local rec;
  local rc = 0;
  for name, value in map.pairs( src ) do
    GP=F and trace("[DEBUG]: <%s:%s>: Processing Pair: Name(%s) Val(%s)",
      MOD, meth, tostring( name ), tostring( value ));
    if( name == "ItemCount" ) then
      GP=F and trace("[DEBUG]<%s:%s>: Processing(%d) Items", MOD, meth, value);
    else
      digestString = name;
      rec = value;
      GP=F and trace("[DEBUG]<%s:%s>: Would have closed SubRec(%s) Rec(%s)",
      MOD, meth, digestString, tostring(rec) );
      -- GP=F and trace("[DEBUG]<%s:%s>: Closing SubRec: Digest(%s) Rec(%s)",
      --   MOD, meth, digestString, tostring(rec) );
      -- rc = aerospike:close_subrec( rec );
      -- GP=F and trace("[DEBUG]<%s:%s>: Closing Results(%d)", MOD, meth, rc );
    end
  end -- for all fields in SRC

  GP=E and trace("[EXIT]: <%s:%s> : RC(%s)", MOD, meth, tostring(rc) );
  -- return rc;
  return 0; -- Mask the error for now:: TODO::@TOBY::Figure this out.
end -- closeAllSubrecs()

-- ===========================
-- End SubRecord Function Area
-- ===========================

-- ======================================================================
-- The value is either simple (atomic) or an object (complex).  Complex
-- objects either have a key function defined, or they have a field called
-- "key" that will give us a key value.
-- If none of these are true -- then return -1 to show our displeasure.
-- ======================================================================
local function getKeyValue( ldtMap, value )
  local meth = "getKeyValue()";
  GP=E and trace("[ENTER]<%s:%s> value(%s) KeyType(%s)",
    MOD, meth, tostring(value), tostring(ldtMap[R_KeyType]) );

  local keyValue;
  if( ldtMap[R_KeyType] == KT_ATOMIC ) then
    keyValue = value;
  else
    -- Employ the user's supplied function (keyFunction) and if that's not
    -- there, look for the special case where the object has a field
    -- called 'key'.  If not, then, well ... tough.  We tried.
    local keyFunction = ldtMap[R_KeyFunction];
    if( keyFunction ~= nil ) and functionTable[keyFunction] ~= nil then
      keyValue = functionTable[keyFunction]( value );
    elseif value["key"] ~= nil then
      keyValue = value["key"];
    else
      keyValue = -1;
    end
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
  return keyValue;
end -- getKeyValue();

-- ======================================================================
-- keyCompare: (Compare ONLY Key values, not Object values)
-- ======================================================================
-- Compare Search Key Value with KeyList, following the protocol for data
-- compare types.  Since compare uses only atomic key types (the value
-- that would be the RESULT of the extractKey() function), we can do the
-- simple compare here, and we don't need "keyType".
-- CR_LESS_THAN    (-1) for searchKey <  dataKey,
-- CR_EQUAL        ( 0) for searchKey == dataKey,
-- CR_GREATER_THAN ( 1) for searchKey >  dataKey
-- Return CR_ERROR (-2) if either of the values is null (or other error)
-- Return CR_INTERNAL_ERROR(-3) if there is some (weird) internal error
-- ======================================================================
local function keyCompare( searchKey, dataKey )
  local meth = "keyCompare()";
  GP=E and trace("[ENTER]<%s:%s> searchKey(%s) data(%s)",
    MOD, meth, tostring(searchKey), tostring(dataKey));

  local result = CR_INTERNAL_ERROR; -- we should never be here.
  -- First check
  if ( dataKey == nil ) then
    warn("[WARNING]<%s:%s> DataKey is nil", MOD, meth );
    result = CR_ERROR;
  elseif( searchKey == nil ) then
    -- a nil search key is always less than everything.
    result = CR_LESS_THAN;
  else
    if searchKey == dataKey then
      result = CR_EQUAL;
    elseif searchKey < dataKey then
        result = CR_LESS_THAN;
    else
      result = CR_GREATER_THAN;
    end
  end

  GP=E and trace("[EXIT]:<%s:%s> Result(%d)", MOD, meth, result );
  return result;
end -- keyCompare()

-- ======================================================================
-- objectCompare: Compare a key with a complex object
-- ======================================================================
-- Compare Search Value with data, following the protocol for data
-- compare types.
-- Parms:
-- (*) ldtMap: control map for LDT
-- (*) searchKey: Key value we're comparing
-- (*) objectValue: Atomic or Complex Object
-- Return:
-- CR_LESS_THAN    (-1) for searchKey <   objectKey
-- CR_EQUAL        ( 0) for searchKey ==  objectKey,
-- CR_GREATER_THAN ( 1) for searchKey >   objectKey
-- Return CR_ERROR (-2) if Key or Object is null (or other error)
-- Return CR_INTERNAL_ERROR(-3) if there is some (weird) internal error
-- ======================================================================
local function objectCompare( ldtMap, searchKey, objectValue )
  local meth = "objectCompare()";
  local keyType = ldtMap[R_KeyType];

  GP=E and trace("[ENTER]<%s:%s> keyType(%s) searchKey(%s) data(%s)",
    MOD, meth, tostring(keyType), tostring(searchKey), tostring(objectValue));

  local result = CR_INTERNAL_ERROR; -- we should never be here.

  -- First check
  if ( objectValue == nil ) then
    warn("[WARNING]<%s:%s> ObjectValue is nil", MOD, meth );
    result = CR_ERROR;
  elseif( searchKey == nil ) then
    GP=F and trace("[INFO]<%s:%s> searchKey is nil:Free Pass", MOD, meth );
    result = CR_EQUAL;
  else
    -- Get the key value for the object -- this could either be the object 
    -- itself (if atomic), or the result of a function that computes the
    -- key from the object.
    local objectKey = getKeyValue( ldtMap, objectValue );

    -- For atomic types (keyType == 0), compare objects directly
    if searchKey == objectKey then
      result = CR_EQUAL;
    elseif searchKey < objectKey then
      result = CR_LESS_THAN;
    else
      result = CR_GREATER_THAN;
    end
  end -- else compare

  GP=E and trace("[EXIT]:<%s:%s> Result(%d)", MOD, meth, result );
  return result;
end -- objectCompare()

-- =======================================================================
--     Node (key) Searching:
-- =======================================================================
--        Index:   1   2   3   4
--     Key List: [10, 20, 30]
--     Dig List: [ A,  B,  C,  D]
--     +--+--+--+                        +--+--+--+
--     |10|20|30|                        |40|50|60| 
--     +--+--+--+                        +--+--+--+
--    / 1 |2 |3  \4 (index)             /   |  |   \
--   A    B  C    D (Digest Ptr)       E    F  G    H
--
--   Child A: all values < 10
--   Child B: all values >= 10 and < 20
--   Child C: all values >= 20 and < 30
--   Child D: all values >= 30
--   (1) Looking for value 15:  (SV=15, Obj=x)
--       : 15 > 10, keep looking
--       : 15 < 20, want Child B (same index ptr as value (2)
--   (2) Looking for value 30:  (SV=30, Obj=x)
--       : 30 > 10, keep looking
--       : 30 > 20, keep looking
--       : 30 = 30, want Child D (same index ptr as value (2)
--   (3) Looking for value 31:  (SV=31, Obj=x)
--       : 31 > 10, keep looking
--       : 31 > 20, keep looking
--       : 31 > 30, At End = want child D
--   (4) Looking for value 5:  (SV=5, Obj=x)
--       : 5 < 10, Want Child A


-- ======================================================================
-- initPropMap( propMap, esrDigest, selfDigest, topDigest, rtFlag, topPropMap )
-- ======================================================================
-- -- Set up the LDR Property Map (one PM per LDT)
-- Parms:
-- (*) propMap: 
-- (*) esrDigest:
-- (*) selfDigest:
-- (*) topDigest:
-- (*) rtFlag:
-- (*) topPropMap:
-- ======================================================================
local function
initPropMap( propMap, esrDigest, selfDigest, topDigest, rtFlag, topPropMap )
  local meth = "initPropMap()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  -- Remember the ESR in the Top Record
  topPropMap[PM_EsrDigest] = esrDigest;

  -- Initialize the PropertyMap of the new ESR
  propMap[PM_EsrDigest]    = esrDigest;
  propMap[PM_RecType  ]    = rtFlag;
  propMap[PM_Magic]        = MAGIC;
  propMap[PM_ParentDigest] = topDigest;
  propMap[PM_SelfDigest]   = selfDigest;
  propMap[PM_CreateTime]   = topPropMap[PM_CreateTime]

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- initPropMap()

-- ======================================================================
-- Create and Init ESR
-- ======================================================================
-- The Existence SubRecord is the synchronization point for the lDTs that
-- have multiple records (one top rec and many children).  It's a little
-- like the baby sitter for the children -- it helps keeps track of them.
-- And, when the ESR is gone, we kill the children. (BRUA-HAHAHAH!!!)
--
-- All LDT subrecs have a properties bin that describes the subrec.  This
-- bin contains a map that is "un-msg-packed" by the C code on the server
-- and read.  It must be the same for all LDT recs.
--
-- ======================================================================
local function createAndInitESR( src, topRec, ldtList )
  local meth = "createAndInitESR()";
  GP=E and trace("[ENTER]<%s:%s> LDT Summary(%s)",
    MOD, meth, ldtSummaryString(ldtList));

  local rc = 0;
  -- Remember to add this to the SRC
  local esrRec       = aerospike:create_subrec( topRec );
  if( esrRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating ESR", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  local esrDigest = record.digest( esrRec );
  local topDigest = record.digest( topRec );
  local topPropMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Init the properties map for this ESR. Note that esrDigest is in here
  -- twice -- once for "self" and once for "esrRec".
  -- Set the Property ControlMap for the ESR, and assign the parent Digest
  -- Note that we use our standard convention for property maps - all subrecs
  -- have a property map, including ESRs.
  local esrPropMap = map();
  initPropMap(esrPropMap, esrDigest, esrDigest, topDigest, RT_ESR, topPropMap);

  esrRec[SUBREC_PROP_BIN] = esrPropMap;
  -- NOTE: We have to make sure that the TopRec propMap also gets saved.

  -- Set the record type as "ESR"
  trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esrRec, RT_ESR );
  trace("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s> Leaving with ESR Digest(%s)",
    MOD, meth, tostring(esrDigest));

  -- Now that it's initialized, add the ESR to the SRC.
  addSubrecToContext( src, esrRec );

  rc = aerospike:update_subrec( esrRec );
  if( rc == nil or rc == 0) then
    -- No longer close via aerospike:close_subrec().
    -- Do that all via the subrecContext
      GP=F and trace("[ESR CLOSE]: <%s:%s> ESR Close postponed", MOD, meth );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  -- After the ESR is all buttoned up -- add it to the SubRec Context.
  addSubrecToContext( src, esrRec );

  trace("[EXIT]<%s:%s> EsrDigest(%s)", MOD, meth, tostring(esrDigest));
  return esrDigest;

end -- createAndInitESR()

-- ======================================================================
-- searchKeyList(): Search the Key list in a Root or Inner Node
-- ======================================================================
-- Search the key list, return the index of the value that represents the
-- child pointer that we should follow.  Notice that this is DIFFERENT
-- from the Leaf Search, which treats the EQUAL case differently.
--
-- For this example:
--              +---+---+---+---+
-- KeyList      |111|222|333|444|
--              +---+---+---+---+
-- DigestList   A   B   C   D   E
--
-- Search Key 100:  Position 1 :: Follow Child Ptr A
-- Search Key 111:  Position 2 :: Follow Child Ptr B
-- Search Key 200:  Position 2 :: Follow Child Ptr B
-- Search Key 222:  Position 2 :: Follow Child Ptr C
-- Search Key 555:  Position 5 :: Follow Child Ptr E
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) keyList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then is always LESS THAN the list
-- Return:
-- OK: Return the Position of the Digest Pointer that we want
-- ERRORS: Return ERR_GENERAL (bad compare)
-- ======================================================================
local function searchKeyList( ldtMap, keyList, searchKey )
  local meth = "searchKeyList()";
  GP=E and trace("[ENTER]<%s:%s>searchKey(%s)", MOD,meth,tostring(searchKey));

  -- We can short-cut this.  If searchKey is nil, then we automatically
  -- return 1 (the first index position).
  if( searchKey == nil ) then
    return 1;
  end

  -- Don't need this at the moment.
  -- local keyType = ldtMap[R_KeyType];

  -- Linear scan of the KeyList.  Find the appropriate entry and return
  -- the index.  Binary Search will come later.
  local resultIndex = 0;
  local compareResult = 0;
  -- Do the List page mode search here
  local listSize = list.size( keyList );
  local listValue;
  for i = 1, listSize, 1 do
    GP=F and trace("[DEBUG]<%s:%s>searchKey(%s) i(%d) keyList(%s)",
    MOD, meth, tostring(searchKey), i, tostring(keyList));

    listValue = keyList[i];
    compareResult = keyCompare( searchKey, listValue );
    if compareResult == CR_ERROR then
      return ERR_GENERAL; -- error result.
    end
    if compareResult  == CR_LESS_THAN then
      -- We want the child pointer that goes with THIS index (left ptr)
      GP=F and trace("[Stop Search: Key < Data]: <%s:%s> : SV(%s) V(%s) I(%d)",
        MOD, meth, tostring(searchKey), tostring( listValue ), i );
        return i; -- Left Child Pointer
    elseif compareResult == CR_EQUAL then
      -- Found it -- return the "right child" index (right ptr)
      GP=F and trace("[FOUND KEY]: <%s:%s> : SrchValue(%s) Index(%d)",
        MOD, meth, tostring(searchKey), i);
      return i + 1; -- Right Child Pointer
    end
    -- otherwise, keep looking.  We haven't passed the spot yet.
  end -- for each list item

  -- Remember: Can't use "i" outside of Loop.   
  GP=F and trace("[FOUND GREATER THAN]: <%s:%s> :Key(%s) Value(%s) Index(%d)",
    MOD, meth, tostring(searchKey), tostring(listValue), listSize + 1 );

  return listSize + 1; -- return furthest right child pointer
end -- searchKeyList()

-- ======================================================================
-- searchObjectList(): Search the Object List in a Leaf Node
-- ======================================================================
-- Search the Object list, return the index of the value that is THE FIRST
-- object to match the search Key. Notice that this method is different
-- from the searchKeyList() -- since that is only looking for the right
-- leaf.  In searchObjectList() we're looking for the actual value.
-- NOTE: Later versions of this method will probably return a location
-- of where to start scanning (for value ranges and so on).  But, for now,
-- we're just looking for an exact match.
-- For this example:
--              +---+---+---+---+
-- ObjectList   |111|222|333|444|
--              +---+---+---+---+
-- Index:         1   2   3   4
--
-- Search Key 100:  Position 1 :: Insert at index location 1
-- Search Key 111:  Position 1 :: Insert at index location 1
-- Search Key 200:  Position 2 :: Insert at index location 2
-- Search Key 222:  Position 2 :: Insert at index location 2
-- Parms:
-- (*) ldtMap: Main control Map
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) objectList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then it compares LESS than everything.
-- Return: Returns a STRUCTURE (a map)
-- (*) POSITION: (where we found it if true, or where we would insert if false)
-- (*) FOUND RESULTS (true, false)
-- (*) ERROR Status: Ok, or Error
--
-- OK: Return the Position of the first matching value.
-- ERRORS:
-- ERR_GENERAL   (-1): Trouble
-- ERR_NOT_FOUND (-2): Item not found.
-- ======================================================================
local function searchObjectList( ldtMap, objectList, searchKey )
  local meth = "searchObjectList()";
  local keyType = ldtMap[R_KeyType];
  GP=E and trace("[ENTER]<%s:%s>searchKey(%s) keyType(%s) ObjList(%s)",
    MOD, meth, tostring(searchKey), tostring(keyType), tostring(objectList));

  local resultMap = map();
  resultMap.Status = ERR_OK;

  -- If we're given a nil searchKey, then we say "found" and return
  -- position 1 -- basically, to set up Scan.
  if( searchKey == nil ) then
    resultMap.Found = true;
    resultMap.Position = 1;
    GP=E and trace("[EARLY EXIT]<%s:%s> SCAN: Nil Key", MOD, meth );
    return resultMap;
  end

  resultMap.Found = false;
  resultMap.Position = 0;

  -- Linear scan of the ObjectList.  Find the appropriate entry and return
  -- the index.  Binary Search will come later.  Binary search is messy with
  -- duplicates.
  local resultIndex = 0;
  local compareResult = 0;
  local objectKey;
  -- Do the List page mode search here
  local listSize = list.size( objectList );

  GP=F and trace("[Starting LOOP]<%s:%s>", MOD, meth );

  for i = 1, listSize, 1 do
    compareResult = objectCompare( ldtMap, searchKey, objectList[i] );
    if compareResult == CR_ERROR then
      resultMap.status = ERR_GENERAL;
      return resultMap;
    end
    if compareResult  == CR_LESS_THAN then
      -- We want the child pointer that goes with THIS index (left ptr)
      GP=F and trace("[NOT FOUND LESS THAN]: <%s:%s> : SV(%s) V(%s) I(%d)",
        MOD, meth, tostring(searchKey), tostring( objectList[i] ), i );
      resultMap.Position = i;
      return resultMap;
    elseif compareResult == CR_EQUAL then
      -- Found it -- return the index of THIS value
      GP=F and trace("[FOUND KEY]: <%s:%s> :Key(%s) Value(%s) Index(%d)",
        MOD, meth, tostring(searchKey), tostring(objectList[i]), i );
      resultMap.Position = i; -- Index of THIS value.
      resultMap.Found = true;
      return resultMap;
    end
    -- otherwise, keep looking.  We haven't passed the spot yet.
  end -- for each list item

  -- Remember: Can't use "i" outside of Loop.   
  GP=F and trace("[NOT FOUND: EOL]: <%s:%s> :Key(%s) Final Index(%d)",
    MOD, meth, tostring(searchKey), listSize );

  resultMap.Position = listSize + 1;
  resultMap.Found = false;

  GP=E and trace("[EXIT]<%s:%s>ResultMap(%s)", MOD,meth,tostring(resultMap));
  return resultMap;
end -- searchObjectList()

-- ======================================================================
-- For debugging purposes, print the tree, starting with the root and
-- then each level down.
-- Root
-- ::Root Children
-- ::::Root Grandchildren
-- :::...::: Leaves
-- ======================================================================
local function printTree( src, topRec, ldtBinName )
  local meth = "printTree()";
  GP=E and trace("[ENTER]<%s:%s> BinName(%s) SRC(%s)",
    MOD, meth, ldtBinName, tostring(src));
  -- Start with the top level structure and descend from there.
  -- At each level, create a new child list, which will become the parent
  -- list for the next level down (unless we're at the leaves).
  -- The root is a special case of a list of parents with a single node.
  local ldtList = topRec[ldtBinName];
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local nodeList = list();
  local childList = list();
  local digestString;
  local nodeRec;
  local treeLevel = ldtMap[R_TreeLevel];

  trace("\n ===========================================================\n");
  trace("\n <PT>begin <PT> <PT>                          <PT> <PT> <PT>\n");
  trace("\n <PT> <PT> <PT>       P R I N T   T R E E     <PT> <PT> <PT>\n");
  trace("\n <PT> <PT> <PT> <PT>                          <PT> <PT> <PT>\n");
  trace("\n ===========================================================\n");

  trace("\n ======  ROOT SUMMARY ======\n(%s)", rootNodeSummary( ldtList ));

  printRoot( topRec, ldtList );

  nodeList = ldtMap[R_RootDigestList];

  -- The Root is already printed -- now print the rest.
  for lvl = 2, treeLevel, 1 do
    local listSize = list.size( nodeList );
    for n = 1, listSize, 1 do
      digestString = tostring( nodeList[n] );
      GP=F and trace("[SUBREC]<%s:%s> OpenSR(%s)", MOD, meth, digestString );
      nodeRec = openSubrec( src, topRec, digestString );
      if( lvl < treeLevel ) then
        -- This is an inner node -- remember all children
        local digestList  = nodeRec[NSR_DIGEST_BIN];
        local digestListSize = list.size( digestList );
        for d = 1, digestListSize, 1 do
          list.append( childList, digestList[d] );
        end -- end for each digest in the node
        printNode( nodeRec );
      else
        -- This is a leaf node -- just print contents of each leaf
        printLeaf( nodeRec );
      end
      GP=F and trace("[SUBREC]<%s:%s> CloseSR(%s)", MOD, meth, digestString );
      -- No -- we don't close.  The SubRecContext handles everything.
      -- aerospike:close_subrec( nodeRec );
    end -- for each node in the list
    -- If we're going around again, then the old childList is the new
    -- ParentList (as in, the nodeList for the next iteration)
    nodeList = childList;
  end -- for each tree level

  trace("\n ===========================================================\n");
  trace("\n <PT> <PT> <PT> <PT> <PT>   E N D   <PT> <PT> <PT> <PT> <PT>\n");
  trace("\n ===========================================================\n");

  GP=E and trace("[EXIT]<%s:%s> ", MOD, meth );
end -- printTree()

-- ======================================================================
-- Update the Leaf Page pointers for a leaf -- used on initial create
-- and leaf splits.  Each leaf has a left and right pointer (digest).
-- Parms:
-- (*) leafRec:
-- (*) leftDigest:  Set PrevPage ptr, if not nil
-- (*) rightDigest: Set NextPage ptr, if not nil
-- ======================================================================
local function setLeafPagePointers( leafRec, leftDigest, rightDigest )
  local meth = "setLeafPagePointers()";
  GP=E and trace("[ENTER]<%s:%s> left(%s) right(%s)",
    MOD, meth, tostring(leftDigest), tostring(rightDigest) );
  leafMap = leafRec[LSR_CTRL_BIN];
  if( leftDigest ~= nil ) then
    leafMap[LF_PrevPage] = leftDigest;
  end
  if( leftDigest ~= nil ) then
    leafMap[LF_NextPage] = rightDigest;
  end
  leafRec[LSR_CTRL_BIN] = leafMap;
  aerospike:update_subrec( leafRec );

  GP=E and trace("[EXIT]<%s:%s> ", MOD, meth );
end -- setLeafPagePointers()

-- ======================================================================
-- We've just done a Leaf split, so now we have to update the page pointers
-- so that the doubly linked leaf page chain remains intact.
-- When we create pages -- we ALWAYS create a new left page (the right one
-- is the previously existing page).  So, the Next Page ptr of the right
-- page is correct (and its right neighbors are correct).  The only thing
-- to change are the LEFT record ptrs -- the new left and the old left.
--      +---+==>+---+==>+---+==>+---+==>
--      | Xi|   |OL |   | R |   | Xj| Leaves Xi, OL, R and Xj
--   <==+---+<==+---+<==+---+<==+---+
--              +---+
--              |NL | Add in this New Left Leaf to be "R"s new left neighbor
--              +---+
--      +---+==>+---+==>+---+==>+---+==>+---+==>
--      | Xi|   |OL |   |NL |   | R |   | Xj| Leaves Xi, OL, NL, R, Xj
--   <==+---+<==+---+<==+---+<==+---+<==+---+
-- Notice that if "OL" exists, then we'll have to open it just for the
-- purpose of updating the page pointer.  This is a pain, BUT, the alternative
-- is even more annoying, which means a tree traversal for scanning.  So
-- we pay our dues here -- and suffer the extra I/O to open the left leaf,
-- so that our leaf page scanning (in both directions) is easy and sane.
-- We are guaranteed that we'll always have a left leaf and a right leaf,
-- so we don't need to check for that.  However, it is possible that if the
-- old Leaf was the left most leaf (what is "R" in this example), then there
-- would be no "OL".  The left leaf digest value for "R" would be ZERO.
--                       +---+==>+---+=+
--                       | R |   | Xj| V
--                     +=+---+<==+---+
--               +---+ V             +---+==>+---+==>+---+=+
-- Add leaf "NL" |NL |     Becomes   |NL |   | R |   | Xj| V
--               +---+             +=+---+<==+---+<==+---+
--                                 V
-- ======================================================================
local function adjustPagePointers( src, topRec, newLeftLeaf, rightLeaf )
  local meth = "adjustPagePointers()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- We'll denote our leaf recs as "oldLeftLeaf, newLeftLeaf and rightLeaf"
  -- The existing rightLeaf points to the oldLeftLeaf.
  local newLeftLeafDigest = record.digest( newLeftLeaf );
  local rightLeafDigest   = record.digest( rightLeaf );

  GP=F and trace("[DEBUG]<%s:%s> newLeft(%s) oldRight(%s)",
    MOD, meth, tostring(newLeftLeafDigest), tostring(rightLeafDigest) );

  local newLeftLeafMap = newLeftLeaf[LSR_CTRL_BIN];
  local rightLeafMap = rightLeaf[LSR_CTRL_BIN];

  local oldLeftLeafDigest = rightLeafMap[LF_PrevPage];
  if( oldLeftLeafDigest == 0 ) then
    -- There is no left Leaf.  Just assign ZERO to the newLeftLeaf Left Ptr
    GP=F and trace("[DEBUG]<%s:%s> No Old Left Leaf (assign ZERO)",MOD, meth );
    newLeftLeafMap[LF_PrevPage] = 0;
  else 
    -- Regular situation:  Go open the old left leaf and update it.
    local oldLeftLeafDigestString = tostring(oldLeftLeafDigest);
    local oldLeftLeaf = openSubrec( src, topRec, oldLeftLeafDigestString );
    if( oldLeftLeaf == nil ) then
      warn("[ERROR]<%s:%s> oldLeftLeaf NIL from openSubrec: digest(%s)",
        MOD, meth, oldLeftLeafDigestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
    local oldLeftLeafMap = oldLeftLeaf[LSR_CTRL_BIN];
    oldLeftLeafMap[LF_NextPage] = newLeftLeafDigest;
    oldLeftLeaf[LSR_CTRL_BIN] = oldLeftLeafMap;
    aerospike:update_subrec( oldLeftLeaf );
    -- Remember, we don't close subrecs.  We use the SRC to do that all 
    -- at the end.
  end

  -- Now update the new Left Leaf, the Right Leaf, and their page ptrs.
  newLeftLeafMap[LF_PrevPage] = oldLeftLeafDigest;
  newLeftLeafMap[LF_NextPage] = rightLeafDigest;
  rightLeafMap[LF_PrevPage]   = newLeftLeafDigest;
  
  -- Save the Leaf Record Maps, and update the subrecs.
  newLeftLeaf[LSR_CTRL_BIN]   =  newLeftLeafMap;
  rightLeaf[LSR_CTRL_BIN]     = rightLeafMap;
  aerospike:update_subrec( newLeftLeaf );
  aerospike:update_subrec( rightLeaf );
  -- Remember:: We use the SRC to close all subrecs at the end.

  GP=E and trace("[EXIT]<%s:%s> ", MOD, meth );
end -- adjustPagePointers()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--    for i = 1, list.size( objectList ), 1 do
--      compareResult = compare( keyType, searchKey, objectList[i] );
--      if compareResult == -2 then
--        return nil -- error result.
--      end
--      if compareResult == 0 then
--        -- Start gathering up values
--        gatherLeafListData( topRec, leafRec, ldtMap, resultList, searchKey,
--          func, fargs, flag );
--        GP=F and trace("[FOUND VALUES]: <%s:%s> : Value(%s) Result(%s)",
--          MOD, meth, tostring(newStorageValue), tostring( resultList));
--          return resultList;
--      elseif compareResult  == 1 then
--        GP=F and trace("[NotFound]: <%s:%s> : Value(%s)",
--          MOD, meth, tostring(newStorageValue) );
--          return resultList;
--      end
--      -- otherwise, keep looking.  We haven't passed the spot yet.
--    end -- for each list item
-- ======================================================================
-- createSearchPath: Create and initialize a search path structure so
-- that we can fill it in during our tree search.
-- Parms:
-- (*) ldtMap: topRec map that holds all of the control values
-- ======================================================================
local function createSearchPath( ldtMap )
  local sp = map();
  sp.LevelCount = 0;
  sp.RecList = list();     -- Track all open nodes in the path
  sp.DigestList = list();  -- The mechanism to open each level
  sp.PositionList = list(); -- Remember where the key was
  sp.HasRoom = list(); -- Check each level so we'll know if we have to split

  -- Cache these here for convenience -- they may or may not be useful
  sp.RootListMax = ldtMap[R_RootListMax];
  sp.NodeListMax = ldtMap[R_NodeListMax];
  sp.LeafListMax = ldtMap[R_LeafListMax];

  return sp;
end -- createSearchPath()

-- ======================================================================
-- updateSearchPath: Rememeber the path that we took during the search
-- so that we can retrace our steps if we need to update the rest of the
-- tree after an insert or delete (although, it's unlikely that we'll do
-- any significant tree change after a delete).
-- Parms:
-- (*) SearchPath: a map that holds all of the secrets
-- (*) propMap: The Property Map (tells what TYPE this record is)
-- (*) ldtMap: Main LDT Control structure
-- (*) nodeRec: a subrec
-- (*) position: location in the current list
-- (*) keyCount: Number of keys in the list
-- ======================================================================
local function
updateSearchPath(sp, propMap, ldtMap, nodeRec, position, keyCount)
  local meth = "updateSearchPath()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> SP(%s) PMap(%s) LMap(%s) Pos(%d) KeyCnt(%d)",
    MOD, meth, tostring(sp), tostring(propMap), tostring(ldtMap),
    position, keyCount);

  local levelCount = sp.LevelCount;
  local nodeRecordDigest = record.digest( nodeRec );
  sp.LevelCount = levelCount + 1;
  list.append( sp.RecList, nodeRec );
  list.append( sp.DigestList, nodeRecordDigest );
  list.append( sp.PositionList, position );
  -- Depending on the Tree Node (Root, Inner, Leaf), we might have different
  -- maximum values.  So, figure out the max, and then figure out if we've
  -- reached it for this node.
  local recType = propMap[PM_RecType];
  local nodeMax = 0;
  if( recType == RT_LDT ) then
      nodeMax = ldtMap[R_RootListMax];
      GP=F and trace("[Root NODE MAX]<%s:%s> Got Max for Root Node(%s)",
        MOD, meth, tostring( nodeMax ));
  elseif( recType == RT_NODE ) then
      nodeMax = ldtMap[R_NodeListMax];
      GP=F and trace("[Inner NODE MAX]<%s:%s> Got Max for Inner Node(%s)",
        MOD, meth, tostring( nodeMax ));
  elseif( recType == RT_LEAF ) then
      nodeMax = ldtMap[R_LeafListMax];
      GP=F and trace("[Leaf NODE MAX]<%s:%s> Got Max for Leaf Node(%s)",
        MOD, meth, tostring( nodeMax ));
  else
      warn("[ERROR]<%s:%s> Bad Node Type (%s) in UpdateSearchPath", 
        MOD, meth, tostring( recType ));
      error( ldte.ERR_INTERNAL );
  end
  GP=F and trace("[HasRoom COMPARE]<%s:%s>KeyCount(%d) NodeListMax(%d)",
    MOD, meth, keyCount, nodeMax );
  if( keyCount >= nodeMax ) then
    list.append( sp.HasRoom, false );
    GP=F and trace("[HasRoom FALSE]<%s:%s>Level(%d) SP(%s)",
        MOD, meth, levelCount,
      tostring( sp ));
  else
    list.append( sp.HasRoom, true );
    GP=F and trace("[HasRoom TRUE ]<%s:%s>Level(%d) SP(%s)",
        MOD, meth, levelCount,
      tostring( sp ));
  end

  GP=E and trace("EXIT UPDATE SEARCH PATH");

  --GP=E and trace("[EXIT]<%s:%s> SP(%s)", MOD, meth, tostring(sp) );
  return rc;
end -- updateSearchPath()

-- ======================================================================
-- scanList(): Scan a List
-- ======================================================================
-- Whether this list came from the Leaf or the Compact List, we'll search
-- thru it and look for matching items -- applying the FILTER on all objects
-- that match the key.
--
-- Parms:
-- (*) objectList
-- (*) startPosition:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchKey:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- Return: A, B, where A is the instruction and B is the return code
-- A: Instruction: 0 (stop), 1 (continue scanning)
-- B: Error Code: B==0 ok.   B < 0 Error.
-- ======================================================================
local function listScan(objectList, startPosition, ldtMap, resultList,
                          searchKey, func, fargs, flag)
  local meth = "listScan()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>StartPosition(%d) SearchKey(%s)",
        MOD, meth, startPosition, tostring( searchKey));

  -- Linear scan of the LIST (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  local uniqueKey = ldtMap[R_KeyUnique];
  local scanStatus = SCAN_CONTINUE;

  local filter = functionTable[func];

  -- Later: Split the loop search into two -- atomic and map objects
  local listSize = list.size( objectList );
  -- We expect that the FIRST compare (at location "start") should be
  -- equal, and then potentially some number of objects after that (assuming
  -- it's NOT a unique key).  If unique, then we will just jump out on the
  -- next compare.
  GP=F and trace("[LIST SCAN]<%s:%s>Position(%d)", MOD, meth, startPosition);
  for i = startPosition, listSize, 1 do
    compareResult = objectCompare( ldtMap, searchKey, objectList[i] );
    if compareResult == CR_ERROR then
      warn("[WARNING]<%s:%s> Compare Error", MOD, meth );
      return 0, CR_ERROR; -- error result.
    end
    if( compareResult == CR_EQUAL ) then
      -- This one qualifies -- save it in result -- if it passes the filter.
      local filterResult = objectList[i];
      if( filter ~= nil ) then
        filterResult = filter( objectList[i], fargs );
      end
      if( filterResult ~= nil ) then
        list.append( resultList, objectList[i] );
        filterPass = true;
      end

      GP=F and trace("[Scan]<%s:%s> Pos(%d) Key(%s) Obj(%s) FilterRes(%s)",
        MOD, meth, i, tostring(searchKey), tostring(objectList[i]),
        tostring(filterResult));

      if( uniqueKey == true and searchKey ~= nil ) then
        scanStatus = SCAN_DONE;
        break;
      end
    else
      -- First non-equals means we're done.
      GP=F and trace("[Scan:NON_MATCH]<%s:%s> Pos(%d) Key(%s) Object(%s)",
        MOD, meth, i, tostring(searchKey), tostring(objectList[i]));
      scanStatus = SCAN_DONE;
      break;
    end
  end -- for each item from startPosition to end

  local resultA = scanStatus;
  local resultB = ERR_OK; -- if we got this far, we're ok.

  GP=E and trace("[EXIT]<%s:%s> rc(%d) resultList(%s) A(%s) B(%s)", MOD, meth,
    rc, tostring(resultList), tostring(resultA), tostring(resultB));
  return resultA, resultB;
end -- listScan()


-- ======================================================================
-- scanByteArray(): Scan a Byte Array, gathering up all of the the
-- matching value(s) in the array.  Before an object can be compared,
-- it must be UN-TRANSFORMED from a binary form to a live object.
-- ======================================================================
-- Parms:
-- (*) byteArray: Packed array of bytes holding transformed objects
-- (*) startPosition: logical ITEM offset (not byte offset)
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchKey:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- Return: A, B, where A is the instruction and B is the return code
-- A: Instruction: 0 (stop), 1 (continue scanning)
-- B: Error Code: B==0 ok.   B < 0 Error.
-- ======================================================================
local function scanByteArray(byteArray, startPosition, ldtMap, resultList,
                          searchKey, func, fargs, flag)
  local meth = "scanByteArray()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>StartPosition(%s) SearchKey(%s)",
        MOD, meth, startPosition, tostring( searchKey));

  -- Linear scan of the ByteArray (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  local uniqueKey = ldtMap[R_KeyUnique];
  local scanStatus = SCAN_CONTINUE;

  -- >>>>>>>>>>>>>>>>>>>>>>>>> BINARY MODE <<<<<<<<<<<<<<<<<<<<<<<<<<<
    -- Do the BINARY (COMPACT BYTE ARRAY) page mode search here -- eventually
  GP=F and warn("[NOTICE!!]: <%s:%s> :BINARY MODE NOT YET IMPLEMENTED",
        MOD, meth, tostring(newStorageValue), tostring( resultList));
  return 0, ERR_GENERAL; -- TODO: Build this mode.

end -- scanByteArray()

-- ======================================================================
-- scanLeaf(): Scan a Leaf Node, gathering up all of the the matching
-- value(s) in the leaf node(s).
-- ======================================================================
-- Once we've searched a B+ Tree and found "The Place", then we have the
-- option of Scanning for values, Inserting new objects or deleting existing
-- objects.  This is the function for gathering up one or more matching
-- values from the leaf node(s) and putting them in the result list.
-- Notice that if there are a LOT Of values that match the search value,
-- then we might read a lot of leaf nodes.
--
-- Leaf Node Structure:
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
-- (*) leafRec:
-- (*) startPosition:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchKey:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- Return: A, B, where A is the instruction and B is the return code
-- A: Instruction: 0 (stop), 1 (continue scanning)
-- B: Error Code: B==0 ok.   B < 0 Error.
-- ======================================================================
-- NOTE: Need to pass in leaf Rec and Start Position -- because the
-- searchPath will be WRONG if we continue the search on a second page.
local function scanLeaf(topRec, leafRec, startPosition, ldtMap, resultList,
                          searchKey, func, fargs, flag)
  local meth = "scanLeaf()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>StartPosition(%s) SearchKey(%s)",
        MOD, meth, startPosition, tostring( searchKey));

  -- Linear scan of the Leaf Node (binary search will come later), for each
  -- match, add to the resultList.
  -- And -- do not confuse binary search (the algorithm for searching the page)
  -- with "Binary Mode", which is how we will compact values into a byte array
  -- for objects that can be transformed into a fixed size object.
  local compareResult = 0;
  local uniqueKey = ldtMap[R_KeyUnique];
  local scanStatus = SCAN_CONTINUE;
  local resultA = 0;
  local resultB = 0;

  if( ldtMap[R_StoreMode] == SM_BINARY ) then
    -- >>>>>>>>>>>>>>>>>>>>>>>>> BINARY MODE <<<<<<<<<<<<<<<<<<<<<<<<<<<
    GP=F and trace("[DEBUG]<%s:%s> BINARY MODE SCAN", MOD, meth );
    local byteArray = leafRec[LSR_BINARY_BIN];
    resultA, resultB = scanByteArray( byteArray, startPosition, ldtMap,
                        resultList, searchKey, func, fargs, flag);
  else
    -- >>>>>>>>>>>>>>>>>>>>>>>>>  LIST  MODE <<<<<<<<<<<<<<<<<<<<<<<<<<<
    GP=F and trace("[DEBUG]<%s:%s> LIST MODE SCAN", MOD, meth );
    -- Do the List page mode search here
    -- Later: Split the loop search into two -- atomic and map objects
    local objectList = leafRec[LSR_LIST_BIN];
    resultA, resultB = listScan(objectList, startPosition, ldtMap,
                  resultList, searchKey, func, fargs, flag);
  end -- else list mode

  GP=E and trace("[EXIT]<%s:%s> rc(%d) resultList(%s) A(%s) B(%s)", MOD, meth,
    rc, tostring(resultList), tostring(resultA), tostring(resultB));
  return resultA, resultB;
end -- scanLeaf()

-- ======================================================================
-- Get the tree node (record) the corresponds to the stated position.
-- ======================================================================
-- local function  getTreeNodeRec( src, topRec, ldtMap, digestList, position )
--   local digestString = tostring( digestList[position] );
--   -- local rec = aerospike:open_subrec( topRec, digestString );
--   local rec = openSubrec( src, topRec, digestString );
--   return rec;
-- end -- getTreeNodeRec()

-- ======================================================================
-- treeSearch( subrecContext, topRec, searchPath, ldtList, searchKey )
-- ======================================================================
-- Search the tree (start with the root and move down). 
-- Remember the search path from root to leaf (and positions in each
-- node) so that insert, Scan and Delete can use this to set their
-- starting positions.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecs
-- (*) topRec: The top level Aerospike Record
-- (*) sp: searchPath: A list of maps that describe each level searched
-- (*) ldtMap: 
-- (*) searchKey: If null, compares LESS THAN everything
-- Return: ST_FOUND(0) or ST_NOTFOUND(-1)
-- And, implicitly, the updated searchPath Object.
local function
treeSearch( src, topRec, sp, ldtList, searchKey )
  local meth = "treeSearch()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> searchKey(%s) ldtSummary(%s)",
      MOD, meth, tostring(searchKey), ldtSummaryString(ldtList) );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  local treeLevels = ldtMap[R_TreeLevel];

  GP=F and trace("[DEBUG]<%s:%s>searchKey(%s) ldtSummary(%s) CMap(%s) PMap(%s)",
      MOD, meth, tostring(searchKey), ldtSummaryString(ldtList),
      tostring(ldtMap), tostring(propMap) );
  -- Start the loop with the special Root, then drop into each successive
  -- inner node level until we get to a LEAF NODE.  We search the leaf node
  -- differently than the inner (and root) nodes, since they have OBJECTS
  -- and not keys.  To search a leaf we must compute the key (from the object)
  -- before we do the compare.
  local keyList = ldtMap[R_RootKeyList];
  local keyCount = list.size( keyList );
  local objectList = nil;
  local objectCount = 0;
  local digestList = ldtMap[R_RootDigestList];
  local position = 0;
  local nodeRec = topRec;
  local nodeMap;
  local resultMap;
  local digestString;

  -- trace("\n\n >> ABOUT TO SEARCH TREE -- Starting with ROOT!!!! \n\n");

  for i = 1, treeLevels, 1 do
    -- trace("\n\n >> SEARCH Loop TOP  << !!!!!!!!!!!!!!!!!!!!!!!!!! \n\n");
    GP=F and trace("[DEBUG]<%s:%s>Loop Iteration(%d) Lvls(%d)",
      MOD, meth, i, treeLevels);
    GP=F and trace("[TREE SRCH] it(%d) Lvls(%d) KList(%s) DList(%s) OList(%s)",
      i, treeLevels, tostring(keyList), tostring(digestList),
      tostring(objectList));
    if( i < treeLevels ) then
      -- It's a root or node search -- so search the keys
      GP=F and trace("[DEBUG]<%s:%s> UPPER NODE Search", MOD, meth );
      position = searchKeyList( ldtMap, keyList, searchKey );
      if( position < 0 ) then
        warn("[ERROR]<%s:%s> searchKeyList Problem", MOD, meth );
        error( ldte.ERR_INTERNAL );
      end
      if( position == 0 ) then
        warn("[ERROR]<%s:%s> searchKeyList Problem:Position ZERO", MOD, meth );
        error( ldte.ERR_INTERNAL );
      end
      updateSearchPath(sp,propMap,ldtMap,nodeRec,position,keyCount );

      -- Get ready for the next iteration.  If the next level is an inner node,
      -- then populate our keyList and nodeMap.
      -- If the next level is a leaf, then populate our ObjectList and LeafMap.
      -- Remember to get the STRING version of the digest in order to
      -- call "open_subrec()" on it.
      GP=F and trace("[DEBUG]Opening Digest Pos(%d) DList(%s) for NextLevel",
        position, tostring( digestList ));

      digestString = tostring( digestList[position] );
      GP=F and trace("[DEBUG]<%s:%s> Checking Next Level", MOD, meth );
      -- NOTE: we're looking at the NEXT level (tl - 1) and we must be LESS
      -- than that to be an inner node.
      if( i < (treeLevels - 1) ) then
        -- Next Node is an Inner Node. 
        GP=F and trace("[Opening NODE Subrec]<%s:%s> Digest(%s) Pos(%d)",
            MOD, meth, digestString, position );
        nodeRec = openSubrec( src, topRec, digestString );
        GP=F and trace("[Open Inner Node Results]<%s:%s>nodeRec(%s)",
          MOD, meth, tostring(nodeRec));
        nodeMap = nodeRec[NSR_CTRL_BIN];
        propMap = nodeRec[SUBREC_PROP_BIN];
        GP=F and trace("[DEBUG]<%s:%s> NEXT NODE: INNER NODE: Summary(%s)",
            MOD, meth, nodeSummaryString( nodeRec ));
        keyList = nodeRec[NSR_KEY_LIST_BIN];
        keyCount = list.size( keyList );
        digestList = nodeRec[NSR_DIGEST_BIN]; 
        GP=F and trace("[DEBUG]<%s:%s> NEXT NODE: Digests(%s) Keys(%s)",
            MOD, meth, tostring( digestList ), tostring( keyList ));
      else
        -- Next Node is a Leaf
        GP=F and trace("[Opening Leaf]<%s:%s> Digest(%s) Pos(%d) TreeLevel(%d)",
          MOD, meth, digestString, position, i+1);
        nodeRec = openSubrec( src, topRec, digestString );
        GP=F and trace("[Open Leaf Results]<%s:%s>nodeRec(%s)",
          MOD,meth,tostring(nodeRec));
        propMap = nodeRec[SUBREC_PROP_BIN];
        nodeMap = nodeRec[LSR_CTRL_BIN];
        GP=F and trace("[DEBUG]<%s:%s> NEXT NODE: LEAF NODE: Summary(%s)",
            MOD, meth, leafSummaryString( nodeRec ));
        objectList = nodeRec[LSR_LIST_BIN];
        objectCount = list.size( objectList );
      end
    else
      -- It's a leaf search -- so search the objects
      GP=F and trace("[DEBUG]<%s:%s> LEAF NODE Search", MOD, meth );
      resultMap = searchObjectList( ldtMap, objectList, searchKey );
      if( resultMap.Status == 0 ) then
        GP=F and trace("[DEBUG]<%s:%s> LEAF Search Result::Pos(%d) Cnt(%d)",
          MOD, meth, resultMap.Position, objectCount);
        updateSearchPath( sp, propMap, ldtMap, nodeRec,
                  resultMap.Position, objectCount );
      else
        GP=F and trace("[SEARCH ERROR]<%s:%s> LeafSrch Result::Pos(%d) Cnt(%d)",
          MOD, meth, resultMap.Position, keyCount);
      end
    end -- if node else leaf.
  end -- end for each tree level

  if( resultMap ~= nil and resultMap.Status == 0 and resultMap.Found == true )
  then
    position = resultMap.Position;
  else
    position = 0;
  end

  if position > 0 then
    rc = ST_FOUND;
  else
    rc = ST_NOTFOUND;
  end

  GP=E and trace("[EXIT]<%s:%s>RC(%d) SearchKey(%s) ResMap(%s) SearchPath(%s)",
      MOD,meth, rc, tostring(searchKey),tostring(resultMap),tostring(sp));

  return rc;
end -- treeSearch()

-- ======================================================================
-- Populate this leaf after a leaf split.
-- Parms:
-- (*) newLeafSubRec
-- (*) objectList
-- ======================================================================
local function populateLeaf( leafRec, objectList )
  local meth = "populateLeaf()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>ObjList(%s)",MOD,meth,tostring(objectList));

  local propMap    = leafRec[SUBREC_PROP_BIN]
  local leafMap    = leafRec[LSR_CTRL_BIN];
  leafRec[LSR_LIST_BIN] = objectList;
  local count = list.size( objectList );
  leafMap[LF_ListEntryCount] = count;
  leafMap[LF_ListEntryTotal] = count;

  leafRec[LSR_CTRL_BIN] = leafMap;
  aerospike:update_subrec( leafRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- populateLeaf()

-- ======================================================================
-- listInsert()
-- General List Insert function that can be used to insert
-- keys, digests or objects.
-- ======================================================================
local function listInsert( myList, newValue, position )
  local meth = "listInsert()";
  GP=E and trace("[ENTER]<%s:%s>List(%s) size(%d) Value(%s) Position(%d)", MOD,
  meth, tostring(myList), list.size(myList), tostring(newValue), position );
  
  local listSize = list.size( myList );
  if( position > listSize ) then
    -- Just append to the list
    list.append( myList, newValue );
    GP=F and trace("[MYLIST APPEND]<%s:%s> Appended item(%s) to list(%s)",
      MOD, meth, tostring(newValue), tostring(myList) );
  else
    -- Move elements in the list from "Position" to the end (end + 1)
    -- and then insert the new value at "Position".  We go back to front so
    -- that we don't overwrite anything.
    -- (move pos:end to the right one cell)
    -- This example: Position = 1, end = 3. (1 based array indexing, not zero)
    --          +---+---+---+
    -- (111) -> |222|333|444| +----Cell added by list.append()
    --          +---+---+---+ V
    --          +---+---+---+---+
    -- (111) -> |   |222|333|444|
    --          +---+---+---+---+
    --          +---+---+---+---+
    --          |111|222|333|444|
    --          +---+---+---+---+
    -- Note that we can't index beyond the end, so that first move must be
    -- an append, not an index access list[end+1] = value.
    GP=F and trace("[MYLIST TRANSFER]<%s:%s> listSize(%d) position(%d)",
      MOD, meth, listSize, position );
    local endValue = myList[listSize];
    list.append( myList, endValue );
    for i = (listSize - 1), position, -1  do
      myList[i+1] = myList[i];
    end -- for()
    myList[position] = newValue;
  end

  GP=E and trace("[EXIT]<%s:%s> Appended(%s) to list(%s)", MOD, meth,
    tostring(newValue), tostring(myList));

  return 0;
end -- listInsert()

-- ======================================================================
-- leafInsert()
-- Use the search position to mark the location where we have to make
-- room for the new value.
-- If we're at the end, we just append to the list.
-- Parms:
-- (*) topRec: Primary Record
-- (*) leafRec: the leaf subrecord
-- (*) ldtMap: LDT Control: needed for key type and storage mode
-- (*) newKey: Search Key for newValue
-- (*) newValue: Object to be inserted.
-- (*) position: If non-zero, then it's where we insert. Otherwise, we search
-- ======================================================================
local function leafInsert( topRec, leafRec, ldtMap, newKey, newValue, position)
  local meth = "leafInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> key(%s) value(%s) ldtMap(%s)",
    MOD, meth, tostring(newKey), tostring(newValue), tostring(ldtMap));

  GP=F and trace("[NOTICE!!]<%s:%s>Using LIST MODE ONLY - No Binary Support (yet)",
    MOD, meth );

  local objectList = leafRec[LSR_LIST_BIN];
  local leafMap =  leafRec[LSR_CTRL_BIN];

  if( position == 0 ) then
    GP=F and trace("[INFO]<%s:%s>Position is ZERO:must Search for position",
      MOD, meth );
    local resultMap = searchObjectList( ldtMap, objectList, newKey );
    position = resultMap.Position;
  end

  if( position <= 0 ) then
    warn("[ERROR]<%s:%s> Search Path Position is wrong", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  -- Move values around, if necessary, to put newValue in a "position"
  rc = listInsert( objectList, newValue, position );

  -- Update Counters
  local itemCount = leafMap[LF_ListEntryCount];
  leafMap[LF_ListEntryCount] = itemCount + 1;
  local totalCount = leafMap[LF_ListEntryTotal];
  leafMap[LF_ListEntryTotal] = totalCount + 1;

  leafRec[LSR_LIST_BIN] = objectList;
  -- Update the leaf record; Close waits until the end
  aerospike:update_subrec( leafRec );
  -- aerospike:close_subrec( leafRec ); -- No longer close here.

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- leafInsert()

-- ======================================================================
-- getNodeSplitPosition()
-- Find the right place to split the B+ Tree Inner Node (or Root)
-- TODO: @TOBY: Maybe find a more optimal split position
-- Right now this is a simple arithmethic computation (split the leaf in
-- half).  This could change to split at a more convenient location in the
-- leaf, especially if duplicates are involved.  However, that presents
-- other problems, so we're doing it the easy way at the moment.
-- Parms:
-- (*) ldtMap: main control map
-- (*) keyList: the key list in the node
-- (*) nodePosition: the place in the key list for the new insert
-- (*) newKey: The new value to be inserted
-- ======================================================================
local function getNodeSplitPosition( ldtMap, keyList, nodePosition, newKey )
  local meth = "getNodeSplitPosition()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );
  GP=F and trace("[NOTICE!!]<%s:%s> Using Rough Approximation", MOD, meth );

  -- This is only an approximization
  local listSize = list.size( keyList );
  local result = (listSize / 2) + 1; -- beginning of 2nd half, or middle

  GP=E and trace("[EXIT]<%s:%s> result(%d)", MOD, meth, result );
  return result;
end -- getNodeSplitPosition

-- ======================================================================
-- getLeafSplitPosition()
-- Find the right place to split the B+ Tree Leaf
-- TODO: @TOBY: Maybe find a more optimal split position
-- Right now this is a simple arithmethic computation (split the leaf in
-- half).  This could change to split at a more convenient location in the
-- leaf, especially if duplicates are involved.  However, that presents
-- other problems, so we're doing it the easy way at the moment.
-- Parms:
-- (*) ldtMap: main control map
-- (*) objList: the object list in the leaf
-- (*) leafPosition: the place in the obj list for the new insert
-- (*) newValue: The new value to be inserted
-- ======================================================================
local function getLeafSplitPosition( ldtMap, objList, leafPosition, newValue )
  local meth = "getLeafSplitPosition()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );
  GP=F and trace("[NOTICE!!]<%s:%s> Using Rough Approximation", MOD, meth );

  -- This is only an approximization
  local listSize = list.size( objList );
  local result = (listSize / 2) + 1; -- beginning of 2nd half, or middle

  GP=E and trace("[EXIT]<%s:%s> result(%d)", MOD, meth, result );
  return result;
end -- getLeafSplitPosition

-- ======================================================================
-- nodeInsert()
-- Insert a new key,digest pair into the node.  We pass in the actual
-- lists, not the nodeRec, so that we can treat Nodes and the Root in
-- the same way.  Thus, it is up to the caller to update the node (or root)
-- information, other than the list update, which is what we do here.
-- Parms:
-- (*) keyList:
-- (*) digestList:
-- (*) key:
-- (*) digest:
-- (*) position:
-- ======================================================================
local function nodeInsert( keyList, digestList, key, digest, position )
  local meth = "nodeInsert()";
  local rc = 0;

  GP=E and trace("[ENTER]<%s:%s> KL(%s) DL(%s) key(%s) D(%s) P(%d)",
    MOD, meth, tostring(keyList), tostring(digestList), tostring(key),
    tostring(digest), position);

  -- If the position is ZERO, then that means we'll have to do another search
  -- here to find the right spot.  Usually, position == 0 means we have
  -- to find the new spot after a split.  Sure, that could be calculated,
  -- but this is safer -- for now.
  if( position == 0 ) then
    GP=F and trace("[INFO]<%s:%s>Position is ZERO:must Search for position", MOD, meth );
    position = searchKeyList( ldtMap, keyList, key );
  end

  -- Move values around, if necessary, to put key and digest in "position"
  rc = listInsert( keyList, key, position );
  rc = listInsert( digestList, digest, position );

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;

end -- nodeInsert()

-- ======================================================================
-- Populate this inner node after a child split.
-- Parms:
-- (*) nodeRec
-- (*) keyList
-- (*) digestList
-- ======================================================================
local function  populateNode( nodeRec, keyList, digestList)
  local meth = "populateNode()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> keyList(%s) digestList(%s)",
    MOD, meth, tostring(keyList), tostring(digestList));

  local nodeItemCount = list.size( keyList );
  nodeRec[NSR_KEY_LIST_BIN] = keyList;
  nodeRec[NSR_DIGEST_BIN] = digestList;

  local nodeMap = nodeRec[NSR_CTRL_BIN];
  nodeMap[ND_ListEntryCount] = nodeItemCount;
  nodeMap[ND_ListEntryTotal] = nodeItemCount;
  nodeRec[NSR_CTRL_BIN] = nodeMap;

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- populateNode()

-- ======================================================================
-- Create a new Inner Node Page and initialize it.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtList: Main LDT Control Structure
-- Contents of a Node Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) NSR_CTRL_BIN:    Main Node Control structure
-- (3) NSR_KEY_LIST_BIN: Key List goes here
-- (4) NSR_DIGEST_BIN: Digest List (or packed binary) goes here
-- (5) NSR_BINARY_BIN:  Packed Binary Array (if used) goes here
-- ======================================================================
local function createNodeRec( src, topRec, ldtList )
  local meth = "createNodeRec()";

  GP=F and trace("\n INSIDE createNodeRec\n");

  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Remember to add this to the SRC
  local nodeRec = aerospike:create_subrec( topRec );
  if( nodeRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating Subrec", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  local rc = initializeNode( topRec, nodeRec, ldtList );
  if( rc >= 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>Node Init OK", MOD, meth );
    rc = aerospike:update_subrec( nodeRec );
  else
    warn("[ERROR]<%s:%s> Problems initializing Node(%d)", MOD, meth, rc );
    error( ldte.ERR_INTERNAL );
  end

  -- Must wait until subRec is initialized before it can be added to SRC.
  -- It should be ready now.
  addSubrecToContext( src, nodeRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return nodeRec;
end -- createNodeRec()


-- ======================================================================
-- splitRootInsert()
-- Split this ROOT node, because after a leaf split and the upward key
-- propagation, there's no room in the ROOT for the additional key.
-- Root Split is different any other node split for several reasons:
-- (1) The Root Key and Digests Lists are part of the control map.
-- (2) The Root stays the root.  We create two new children (inner nodes)
--     that become a new level in the tree.
-- Parms:
-- (*) src: SubRec Context (for looking up open subrecs)
-- (*) topRec:
-- (*) sp: SearchPath (from the initial search)
-- (*) ldtList:
-- (*) key:
-- (*) digest:
-- ======================================================================
local function splitRootInsert( src, topRec, sp, ldtList, key, digest )
  local meth = "splitRootInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> topRec(%s) SRC(%s) SP(%s) LDT(%s) Key(%s) ",
    MOD, meth,tostring(topRec), tostring(src), tostring(sp), tostring(key),
    tostring(digest));
  
  GP=F and trace("\n\n <><H><> !!! SPLIT ROOT !!! Key(%s)<><W><> \n",
    tostring( key ));

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  local rootLevel = 1;
  local rootPosition = sp.PositionList[rootLevel];

  local keyList = ldtMap[R_RootKeyList];
  local digestList = ldtMap[R_RootDigestList];

  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition =
      getNodeSplitPosition( ldtMap, keyList, rootPosition, key );
  -- local splitKey = getKeyValue( ldtMap, keyList[splitPosition] );
  local splitKey = keyList[splitPosition];

  GP=F and trace("[STATUS]<%s:%s> Take and Drop::Pos(%d)Key(%s) Digest(%s)",
    MOD, meth, splitPosition, tostring(keyList), tostring(digestList));

    -- Splitting a node works as follows.  The node is split into a left
    -- piece, a right piece, and a center value that is propagated up to
    -- the parent (in this case, root) node.
    --              +---+---+---+---+---+
    -- KeyList      |111|222|333|444|555|
    --              +---+---+---+---+---+
    -- DigestList   A   B   C   D   E   F
    --
    --                      +---+
    -- New Parent Element   |333|
    --                      +---+
    --                     /     \
    --              +---+---+   +---+---+
    -- KeyList      |111|222|   |444|555|
    --              +---+---+   +---+---+
    -- DigestList   A   B   C   D   E   F
    --
  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- will let us split the current Root node list into two node lists.
  -- We propagate up the split key (the new root value) and the two
  -- new inner node digests.
  local leftKeyList  = list.take( keyList, splitPosition - 1 );
  local rightKeyList = list.drop( keyList, splitPosition  );

  local leftDigestList  = list.take( digestList, splitPosition );
  local rightDigestList = list.drop( digestList, splitPosition );

  GP=F and trace("\n[DEBUG]<%s:%s>LKey(%s) LDig(%s) SKey(%s) RKey(%s) RDig(%s)",
    MOD, meth, tostring(leftKeyList), tostring(leftDigestList),
    tostring( splitKey ), tostring(rightKeyList), tostring(rightDigestList) );

  -- Create two new Child Inner Nodes -- that will be the new Level 2 of the
  -- tree.  The root gets One Key and Two Digests.
  local leftNodeRec  = createNodeRec( src, topRec, ldtList );
  local rightNodeRec = createNodeRec( src, topRec, ldtList );

  local leftNodeDigest  = record.digest( leftNodeRec );
  local rightNodeDigest = record.digest( rightNodeRec );

  -- This is a different order than the splitLeafInsert, but before we
  -- populate the new child nodes with their new lists, do the insert of
  -- the new key/digest value now.
  -- Figure out WHICH of the two nodes that will get the new key and
  -- digest. Insert the new value.
  -- Compare against the SplitKey -- if less, insert into the left node,
  -- and otherwise insert into the right node.
  local compareResult = keyCompare( key, splitKey );
  if( compareResult == CR_LESS_THAN ) then
    -- We choose the LEFT Node -- but we must search for the location
    nodeInsert( leftKeyList, leftDigestList, key, digest, 0 );
  elseif( compareResult >= CR_EQUAL  ) then -- this works for EQ or GT
    -- We choose the RIGHT (new) Node -- but we must search for the location
    nodeInsert( rightKeyList, rightDigestList, key, digest, 0 );
  else
    -- We got some sort of goofy error.
    warn("[ERROR]<%s:%s> Compare Error(%d)", MOD, meth, compareResult );
    error( ldte.ERR_INTERNAL );
  end

  -- Populate the new nodes with their Key and Digest Lists
  populateNode( leftNodeRec, leftKeyList, leftDigestList);
  populateNode( rightNodeRec, rightKeyList, rightDigestList);
  aerospike:update_subrec( leftNodeRec );
  aerospike:update_subrec( rightNodeRec );

  -- Replace the Root Information with just the split-key and the
  -- two new child node digests (much like first Tree Insert).
  local keyList = list();
  list.append( keyList, splitKey );
  local digestList = list();
  list.append( digestList, leftNodeDigest );
  list.append( digestList, rightNodeDigest );

  -- The new tree is now one level taller
  local treeLevel = ldtMap[R_TreeLevel];
  ldtMap[R_TreeLevel] = treeLevel + 1;

  -- Update the Main control map with the new root lists.
  ldtMap[R_RootKeyList] = keyList;
  ldtMap[R_RootDigestList] = digestList;

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- splitRootInsert()

-- ======================================================================
-- splitNodeInsert()
-- Split this parent node, because after a leaf split and the upward key
-- propagation, there's no room in THIS node for the additional key.
-- Special case is "Root Split" -- and that's handled by the function above.
-- Just like the leaf split situation -- we have to be careful about 
-- duplicates.  We don't want to split in the middle of a set of duplicates,
-- if we can possibly avoid it.  If the WHOLE node is the same key value,
-- then we can't avoid it.
-- Parms:
-- (*) src: SubRec Context (for looking up open subrecs)
-- (*) topRec:
-- (*) sp: SearchPath (from the initial search)
-- (*) ldtList:
-- (*) key:
-- (*) digest:
-- (*) level:
-- ======================================================================
local function splitNodeInsert( src, topRec, sp, ldtList, key, digest, level )
  local meth = "splitNodeInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> SRC(%s) SP(%s) LDT(%s) Key(%s) Lvl(%d)",
    MOD, meth, tostring(src), tostring(sp), tostring(key), tostring(digest),
    level );
  
  if( level == 1 ) then
    -- Special Split -- Root is handled differently.
    rc = splitRootInsert( src, topRec, sp, ldtList, key, digest );
  else
    -- Ok -- "Regular" Inner Node Split Insert.
    -- We will split this inner node, use the existing node as the new
    -- "rightNode" and the newly created node as the new "LeftNode".
    -- We will insert the "splitKey" and the new leftNode in the parent.
    -- And, if the parent has no room, we'll recursively call this function
    -- to propagate the insert up the tree.  ((I hope recursion doesn't
    -- blow up the Lua environment!!!!  :-) ).

    GP=F and trace("\n\n <><!><> !!! SPLIT INNER NODE !!! <><E><> \n\n");

    -- Extract the property map and control map from the ldt bin list.
    local propMap = ldtList[1];
    local ldtMap  = ldtList[2];
    local binName = propMap[PM_BinName];

    local nodePosition = sp.PositionList[level];
    local nodeRecDigest = sp.DigestList[lnodeevel];
    local nodeRec = sp.RecList[level];

    -- Open the Node get the map, Key and Digest Data
    local nodePropMap    = nodeRec[SUBREC_PROP_BIN];
    GP=F and trace("\n[DUMP]<%s:%s>Node Prop Map(%s)", MOD, meth, tostring(nodePropMap));

    local nodeMap    = nodeRec[NSR_CTRL_BIN];
    local keyList    = nodeRec[NSR_KEY_LIST_BIN];
    local digestList = nodeRec[NSR_DIGEST_BIN];

    -- Calculate the split position and the key to propagate up to parent.
    local splitPosition =
        getNodeSplitPosition( ldtMap, keyList, nodePosition, key );
    -- We already have a key list -- don't need to "extract".
    -- local splitKey = getKeyValue( ldtMap, keyList[splitPosition] );
    local splitKey = keyList[splitPosition];

    GP=F and trace("\n[DUMP]<%s:%s> Take and Drop:: Map(%s) KeyList(%s) DigestList(%s)",
      MOD, meth, tostring(nodeMap), tostring(keyList), tostring(digestList));

    -- Splitting a node works as follows.  The node is split into a left
    -- piece, a right piece, and a center value that is propagated up to
    -- the parent node.
    --              +---+---+---+---+---+
    -- KeyList      |111|222|333|444|555|
    --              +---+---+---+---+---+
    -- DigestList   A   B   C   D   E   F
    --
    --                      +---+
    -- New Parent Element   |333|
    --                      +---+
    --                     /     \
    --              +---+---+   +---+---+
    -- KeyList      |111|222|   |444|555|
    --              +---+---+   +---+---+
    -- DigestList   A   B   C   D   E   F
    --
    -- Our List operators :
    -- (*) list.take (take the first N elements) 
    -- (*) list.drop (drop the first N elements, and keep the rest) 
    -- will let us split the current Node list into two Node lists.
    -- We will always propagate up the new Key and the NEW left page (digest)
    local leftKeyList  = list.take( keyList, splitPosition - 1 );
    local rightKeyList = list.drop( keyList, splitPosition );

    local leftDigestList  = list.take( digestList, splitPosition );
    local rightDigestList = list.drop( digestList, splitPosition );

    GP=F and trace("\n[DEBUG]<%s:%s>: LeftKey(%s) LeftDig(%s) RightKey(%s) RightDig(%s)",
      MOD, meth, tostring(leftKeyList), tostring(leftDigestList),
      tostring(rightKeyList), tostring(rightDigestList) );

    local rightNodeRec = nodeRec; -- our new name for the existing node
    local leftNodeRec = createNodeRec( src, topRec, ldtList );
    local leftNodeDigest = record.digest( leftNodeRec );

    -- This is a different order than the splitLeafInsert, but before we
    -- populate the new child nodes with their new lists, do the insert of
    -- the new key/digest value now.
    -- Figure out WHICH of the two nodes that will get the new key and
    -- digest. Insert the new value.
    -- Compare against the SplitKey -- if less, insert into the left node,
    -- and otherwise insert into the right node.
    local compareResult = keyCompare( key, splitKey );
    if( compareResult == CR_LESS_THAN ) then
      -- We choose the LEFT Node -- but we must search for the location
      nodeInsert( leftKeyList, leftDigestList, key, digest, 0 );
    elseif( compareResult >= CR_EQUAL  ) then -- this works for EQ or GT
      -- We choose the RIGHT (new) Node -- but we must search for the location
      nodeInsert( rightKeyList, rightDigestList, key, digest, 0 );
    else
      -- We got some sort of goofy error.
      warn("[ERROR]<%s:%s> Compare Error(%d)", MOD, meth, compareResult );
      error( ldte.ERR_INTERNAL );
    end

    -- Populate the new nodes with their Key and Digest Lists
    populateNode( leftNodeRec, leftKeyList, leftDigestList);
    populateNode( rightNodeRec, rightKeyList, rightDigestList);
    aerospike:update_subrec( leftNodeRec );
    aerospike:update_subrec( rightNodeRec );

    -- Update the parent node with the new Node information.  It is the job
    -- of this method to either split the parent or do a straight insert.
    
    GP=F and trace("\n\n CALLING INSERT PARENT FROM SPLIT NODE: Key(%s)\n",
      tostring(splitKey));

    insertParentNode(src, topRec, sp, ldtList, splitKey,
      leftNodeDigest, level - 1 );
  end -- else regular (non-root) node split

  GP=F and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;

end -- splitNodeInsert()

-- ======================================================================
-- After a leaf split or a node split, this parent node gets a new child
-- value and digest.  This node might be the root, or it might be an
-- inner node.  If we have to split this node, then we'll perform either
-- a node split or a ROOT split (ugh) and recursively call this method
-- to insert one level up.  Of course, Root split is a special case, because
-- the root node is basically ensconced inside of the LDT control map.
-- Parms:
-- (*) src: The SubRec Context (holds open subrecords).
-- (*) topRec: The main record
-- (*) sp: the searchPath structure
-- (*) ldtList: the main control structure
-- (*) key: the new key to be inserted
-- (*) digest: The new digest to be inserted
-- (*) level: The current level in searchPath of this node
-- ======================================================================
-- NOTE: This function is FORWARD-DECLARED, so it does NOT get a "local"
-- declaration here.
-- ======================================================================
function insertParentNode(src, topRec, sp, ldtList, key, digest, level)
  local meth = "insertParentNode()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s> SP(%s) Key(%s) Dig(%s) Level(%d)",
    MOD, meth, tostring(sp), tostring(key), tostring(digest), level );
  GP=F and trace("\n[DUMP]<%s> LDT(%s)", meth, ldtSummaryString(ldtList) );

  GP=F and trace("\n\n STARTING INTO INSERT PARENT NODE \n\n");

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- Check the tree level.  If it's the root, we access the node data
  -- differently from a regular inner tree node.
  local listMax;
  local keyList;
  local digestList;
  local position = sp.PositionList[level];
  local nodeRec = nil;
  GP=F and trace("[DEBUG]<%s:%s> Lvl(%d) Pos(%d)", MOD, meth, level, position);
  if( level == 1 ) then
    -- Get the control and list data from the Root Node
    listMax    = ldtMap[R_RootListMax];
    keyList    = ldtMap[R_RootKeyList];
    digestList = ldtMap[R_RootDigestList];
  else
    -- Get the control and list data from a regular inner Tree Node
    nodeRec = sp.RecList[level];
    if( nodeRec == nil ) then
      warn("[ERROR]<%s:%s> Nil NodeRec from SearchPath. Level(%s)",
        MOD, meth, tostring(level));
      error( ldte.ERR_INTERNAL );
    end
    listMax    = ldtMap[R_NodeListMax];
    keyList    = nodeRec[NSR_KEY_LIST_BIN];
    digestList = nodeRec[NSR_DIGEST_BIN];
  end

  -- If there's room in this node, then this is easy.  If not, then
  -- it's a complex split and propagate.
  if( sp.HasRoom[level] == true ) then
    -- Regular node insert
    rc = nodeInsert( keyList, digestList, key, digest, position );
    -- If it's a node, then we have to re-assign the list to the subrec
    -- fields -- otherwise, the change may not take effect.
    if( rc == 0 ) then
      if( level > 1 ) then
        nodeRec[NSR_KEY_LIST_BIN] = keyList;
        nodeRec[NSR_DIGEST_BIN]   = digestList;
        aerospike:update_subrec( nodeRec );
      end
    else
      -- Bummer.  Errors.
      warn("[ERROR]<%s:%s> Parent Node Errors in NodeInsert", MOD, meth );
      error( ldte.ERR_INTERNAL );
    end
  else
    -- Complex node split and propagate up to parent.  Special case is if
    -- this is a ROOT split, which is different.
    rc = splitNodeInsert( src, topRec, sp, ldtList, key, digest, level);
  end

  GP=F and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- insertParentNode()

-- ======================================================================
-- Create a new Leaf Page and initialize it.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtList: Main LDT Control Structure
-- (*) firstValue: if present, insert this value
-- NOTE: Remember that we must create an ESR when we create the first leaf
-- but that is the caller's job
-- Contents of a Leaf Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) LSR_CTRL_BIN:    Main Leaf Control structure
-- (3) LSR_LIST_BIN:    Object List goes here
-- (4) LSR_BINARY_BIN:  Packed Binary Array (if used) goes here
-- ======================================================================
local function createLeafRec( src, topRec, ldtList, firstValue )
  local meth = "createLeafRec()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Remember to add this to the SRC
  local leafRec = aerospike:create_subrec( topRec );
  if( leafRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating Subrec", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  -- Increase the Subrec Count
  local subrecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subrecCount + 1;

  local rc = initializeLeaf( topRec, ldtList, leafRec, firstValue );
  if( rc >= 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>Leaf Init OK", MOD, meth );
    rc = aerospike:update_subrec( leafRec );
  else
    warn("[ERROR]<%s:%s> Problems initializing Leaf(%d)", MOD, meth, rc );
    error( ldte.ERR_INTERNAL );
  end

  -- Must wait until subRec is initialized before it can be added to SRC.
  -- It should be ready now.
  addSubrecToContext( src, leafRec );

  GP=F and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return leafRec;
end -- createLeafRec()

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
-- For now, we'll make the split easier and just pick the middle item,
-- but in doing that, it will make the scanning more complicated.
-- Parms:
-- (*) src: subrecContext
-- (*) topRec
-- (*) sp: searchPath
-- (*) ldtList
-- (*) newKey
-- (*) newValue
-- Return:
-- ======================================================================
local function
splitLeafInsert( src, topRec, sp, ldtList, newKey, newValue )
  local meth = "splitLeafInsert()";
  local rc = 0;

  GP=F and trace("\n\n <><><> !!! SPLIT LEAF !!! <><><> \n\n");

  GP=E and trace("[ENTER]<%s:%s> SP(%s) LDT(%s) Key(%s) Val(%s)",
    MOD, meth, tostring(sp), ldtSummaryString(ldtList),
    tostring(newKey), tostring(newValue));

  -- Splitting a leaf works as follows.  It is slightly different than a
  -- node split.  The leaf is split into a left piece and a right piece. 
  --
  -- The first element if the right leaf becomes the new key that gets
  -- propagated up to the parent.  This is the main difference between a Leaf
  -- split and a node split.  The leaf split is the COPY of the key, whereas
  -- the node split moves the key up.
  --               +---+---+
  --  Key List     |111|888|
  --               +---+---+
  --  Digest List  A   B   C
  --
  -- +---+---+    +---+---+---+---+---+    +---+---+
  -- | 50| 88|    |111|222|333|444|555|    |888|999|
  -- +---+---+    +---+---+---+---+---+    +---+---+
  -- Leaf A       Leaf B                   Leaf C
  --
  --                      +---+
  -- New Parent Element   |333|
  --                      +---+
  --                     /     \
  --              +---+---+   +---+---+---+
  --              |111|222|   |333|444|555|
  --              +---+---+   +---+---+---+
  --              Leaf B1     Leaf B2
  --
  --               +---+---+---+
  --  Key List     |111|333|888|
  --               +---+---+---+
  --  Digest List  A   B1  B2  C
  --
  -- +---+---+    +---+---+   +---+---+---+    +---+---+
  -- | 50| 88|    |111|222|   |333|444|555|    |888|999|
  -- +---+---+    +---+---+   +---+---+---+    +---+---+
  -- Leaf A       Leaf B1     Leaf B2          Leaf C
  --
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  local leafLevel = sp.LevelCount;
  local leafPosition = sp.PositionList[leafLevel];
  local leafRecDigest = sp.DigestList[leafLevel];
  local leafRec = sp.RecList[leafLevel];

  -- Open the Leaf and look inside.
  local leafMap    = leafRec[LSR_CTRL_BIN];
  local objectList = leafRec[LSR_LIST_BIN];

  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition =
      getLeafSplitPosition( ldtMap, objectList, leafPosition, newValue );
  local splitKey = getKeyValue( ldtMap, objectList[splitPosition] );

  GP=F and trace("[STATUS]<%s:%s> About to Take and Drop:: List(%s)", MOD, meth,
    tostring(objectList));

  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- will let us split the current leaf list into two leaf lists.
  -- We will always propagate up the new Key and the NEW left page (digest)
  local leftList  = list.take( objectList, splitPosition - 1 );
  local rightList = list.drop( objectList, splitPosition - 1 );

  GP=F and trace("\n[DEBUG]<%s:%s>: LeftList(%s) SplitKey(%s) RightList(%s)",
    MOD, meth, tostring(leftList), tostring(splitKey), tostring(rightList) );

  local rightLeafRec = leafRec; -- our new name for the existing leaf
  local leftLeafRec = createLeafRec( src, topRec, ldtList, nil );
  local leftLeafDigest = record.digest( leftLeafRec );

  -- Overwrite the leaves with their new object value lists
  populateLeaf( leftLeafRec, leftList );
  populateLeaf( rightLeafRec, rightList );

  -- Update the Page Pointers: Given that they are doubly linked, we can
  -- easily find the ADDITIONAL page that we have to open so that we can
  -- update its next-page link.  If we had to go up and down the tree to find
  -- it (the near LEFT page) that would be a horrible HORRIBLE experience.
  rc = adjustPagePointers( src, topRec, leftLeafRec, rightLeafRec );

  -- Now figure out WHICH of the two leaves (original or new) we have to
  -- insert the new value.
  -- Compare against the SplitKey -- if less, insert into the left leaf,
  -- and otherwise insert into the right leaf.
  local compareResult = keyCompare( newKey, splitKey );
  if( compareResult == CR_LESS_THAN ) then
    -- We choose the LEFT Leaf -- but we must search for the location
    leafInsert( topRec, leftLeafRec, ldtMap, newKey, newValue, 0);
  elseif( compareResult >= CR_EQUAL  ) then -- this works for EQ or GT
    -- We choose the RIGHT (new) Leaf -- but we must search for the location
    leafInsert( topRec, rightLeafRec, ldtMap, newKey, newValue, 0);
  else
    -- We got some sort of goofy error.
    warn("[ERROR]<%s:%s> Compare Error(%d)", MOD, meth, compareResult );
    error( ldte.ERR_INTERNAL );
  end

  aerospike:update_subrec( leftLeafRec );
  aerospike:update_subrec( rightLeafRec );

  -- Update the parent node with the new leaf information.  It is the job
  -- of this method to either split the parent or do a straight insert.
  GP=F and trace("\n\n CALLING INSERT PARENT FROM SPLIT LEAF: Key(%s)\n",
    tostring(splitKey));
  insertParentNode(src, topRec, sp, ldtList, splitKey,
    leftLeafDigest, leafLevel - 1 );

  GP=F and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- splitLeafInsert()

-- ======================================================================
-- firstTreeInsert( topRec, ldtList, newValue, stats )
-- ======================================================================
-- For the VERY FIRST INSERT, we don't need to search.  We just put the
-- first key in the root, and we allocate TWO leaves: the left leaf for
-- values LESS THAN the first value, and the right leaf for values
-- GREATER THAN OR EQUAL to the first value.
-- Parms:
-- (*) src: SubRecContext
-- (*) topRec
-- (*) ldtList
-- (*) newValue
-- (*) stats: bool: When true, we update stats
local function firstTreeInsert( src, topRec, ldtList, newValue, stats )
  local meth = "firstTreeInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>LdtSummary(%s) newValue(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue) );

  -- We know that on the VERY FIRST SubRecord create, we want to create
  -- the Existence Sub Record (ESR).  So, do this first.
  local esrDigest = createAndInitESR( src, topRec, ldtList );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  local rootKeyList = ldtMap[R_RootKeyList];
  local rootDigestList = ldtMap[R_RootDigestList];
  local keyValue = getKeyValue( ldtMap, newValue );

  -- Create two leaves -- Left and Right. Initialize them.  Then
  -- insert our new value into the RIGHT one.
  local leftLeafRec = createLeafRec( src, topRec, ldtList, nil );
  local leftLeafDigest = record.digest( leftLeafRec );

  local rightLeafRec = createLeafRec( src, topRec, ldtList, newValue);
  local rightLeafDigest = record.digest( rightLeafRec );

  -- Our leaf pages are doubly linked -- we use digest values as page ptrs.
  setLeafPagePointers( leftLeafRec, 0, rightLeafDigest );
  setLeafPagePointers( rightLeafRec, leftLeafDigest, 0 );

  GP=F and trace("[DEBUG]<%s:%s>Created Left(%s) and Right(%s) Records",
    MOD, meth, tostring(leftLeafDigest), tostring(rightLeafDigest) );

  -- Insert our very first key into the root directory (no search needed),
  -- along with the two new child digests
  list.append( rootKeyList, keyValue );
  list.append( rootDigestList, leftLeafDigest );
  list.append( rootDigestList, rightLeafDigest );

  if( stats == true ) then
    local totalCount = ldtMap[R_TotalCount];
    ldtMap[R_TotalCount] = totalCount + 1;
    local itemCount = propMap[PM_ItemCount];
    propMap[PM_ItemCount] = itemCount + 1;
  end

  ldtMap[R_TreeLevel] = 2; -- We can do this blind, since it's special.

  -- Still experimenting -- not sure how much we have to "reset", but some
  -- things are not currently being updated correctly.
  -- TODO: @TOBY: Double check this and fix.
  ldtMap[R_RootKeyList] = rootKeyList;
  ldtMap[R_RootDigestList] = rootDigestList;
  ldtList[2] = ldtMap;
  topRec[binName] = ldtList;

  -- Note: The caller will update the top record, but we need to update
  -- and close the subrecs here.
  aerospike:update_subrec( leftLeafRec );
  -- aerospike:close_subrec( leftLeafRec );
  aerospike:update_subrec( rightLeafRec );
  -- aerospike:close_subrec( rightLeafRec );

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) newValue(%s) rc(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue), tostring(rc));
  return rc;
end -- firstTreeInsert()

-- ======================================================================
-- treeInsert( src, topRec, ldtList, value, stats )
-- ======================================================================
-- Search the tree (start with the root and move down).  Get the spot in
-- the leaf where the insert goes.  Insert into the leaf.  Remember the
-- path on the way down, because if a leaf splits, we have to move back
-- up and potentially split the parents bottom up.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec
-- (*) ldtList
-- (*) value
-- (*) stats: bool: When true, we update stats
local function treeInsert( src, topRec, ldtList, value, stats )
  local meth = "treeInsert()";
  local rc = 0;
  
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  GP=F and trace("[PARMS]<%s:%s>value(%s) stats(%s) LdtSummary(%s) ",
  MOD, meth, tostring(value), tostring(stats), ldtSummaryString(ldtList));

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  local key = getKeyValue( ldtMap, value );

  -- For the VERY FIRST INSERT, we don't need to search.  We just put the
  -- first key in the root, and we allocate TWO leaves: the left leaf for
  -- values LESS THAN the first value, and the right leaf for values
  -- GREATER THAN OR EQUAL to the first value.
  -- Note that later -- when we do a batch insert -- this will be smarter.
  if( ldtMap[R_TreeLevel] == 1 ) then
    GP=F and trace("[DEBUG]<%s:%s>\n\n<FFFF> FIRST TREE INSERT!!!\n",
        MOD, meth );
    firstTreeInsert( src, topRec, ldtList, value, stats );
  else
    GP=F and trace("[DEBUG]<%s:%s>\n\n<RRRR> Regular TREE INSERT(%s)!!!\n\n",
        MOD, meth, tostring(value));
    -- It's a real insert -- so, Search first, then insert
    -- Map: Path from root to leaf, with indexes
    -- The Search path is a map of values, including lists from root to leaf
    -- showing node/list states, counts, fill factors, etc.
    local sp = createSearchPath(ldtMap);
    local status =
      treeSearch( src, topRec, sp, ldtList, key );

    if( status == ST_FOUND and ldtMap[R_KeyUnique] == true ) then
      warn("[User ERROR]<%s:%s> Unique Key(%s) Violation",
        MOD, meth, tostring(value ));
      error( ldte.ERR_UNIQUE_KEY );
    end
    local leafLevel = sp.LevelCount;

    GP=F and trace("[DEBUG]<%s:%s>LeafInsert: Level(%d): HasRoom(%s)",
      MOD, meth, leafLevel, tostring(sp.HasRoom[leafLevel] ));

    if( sp.HasRoom[leafLevel] == true ) then
      -- Regular Leaf Insert
      local leafRec = sp.RecList[leafLevel];
      local position = sp.PositionList[leafLevel];
      rc = leafInsert( topRec, leafRec, ldtMap, key, value, position);
      aerospike:update_subrec( leafRec );
    else
      -- Split first, then insert.  This split can potentially propagate all
      -- the way up the tree to the root. This is potentially a big deal.
      rc = splitLeafInsert( src, topRec, sp, ldtList,
                            key, value );
    end
  end -- end else "real" insert

  -- All of the subrecords were written out in the respective insert methods,
  -- so if all went well, we'll now update the top record. Otherwise, we
  -- will NOT udate it.
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>::Updating TopRec: rc(%s)",
      MOD, meth, tostring( rc ));
    rc = aerospike:update( topRec );
  else
    warn("[ERROR]<%s:%s>Insert Error::Ldt(%s) value(%s) stats(%s) rc(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(value), tostring(stats),
    tostring(rc));
    error( ldte.ERR_INSERT );
  end

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) value(%s) rc(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(value), tostring(rc));
  return rc;
end -- treeInsert

-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the ldtMap, if present, and apply
-- it to the value, returning the transformed value.  If no transform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyTransform( transformFunc, value )
  local meth = "applyTransform()";
  GP=E and trace("[ENTER]<%s:%s> transform(%s) type(%s) Value(%s)",
 MOD, meth, tostring(transformFunc), type(transformFunc), tostring(value));

  local storeValue = value;
  if transformFunc ~= nil then 
    storeValue = transformFunc( value );
  end

  GP=F and trace("[EXIT]<%s:%s>storeValue(%s)",MOD,meth,tostring(storeValue));
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
  local meth = "applyUnTransform()";
  GP=E and trace("[ENTER]<%s:%s>storeValue(%s)",MOD,meth,tostring(storeValue));

  local returnValue = storeValue;
  if ldtMap[R_UnTransform] ~= nil and
    functionTable[ldtMap[R_UnTransform]] ~= nil then
    returnValue = functionTable[ldtMap[R_UnTransform]]( storeValue );
  end
  GP=F and trace("[EXIT]<%s:%s>RetValue(%s)",MOD,meth,tostring(returnValue));
  return returnValue;
end -- applyUnTransform( value )

-- =======================================================================
-- unTransformSimpleCompare()
-- Apply the unTransform function (if not nil) and perform an EQUAL compare
-- on the key and DB Value.  Note that we are just doing equals here of
-- simple types, so we can just use the equals (==) operator.
-- Return the unTransformed search value if the values match.
-- =======================================================================
local function unTransformSimpleCompare(unTransform, dbValue, key)
  local meth = "unTransformSimpleCompare()";
  GP=E and trace("[ENTER]<%s:%s> storeVal(%s) Key(%s)",
    MOD, meth, tostring(dbValue), tostring(key));

  local modDbValue = dbValue;
  local resultValue = nil;
  local compareResult = false; -- used for debugging (remove later)

  if unTransform ~= nil then
    modDbValue = unTransform( dbValue );
  end

  -- If we have NO search Key, or it matches the result, we have a match.
  if( key == nil or key == modDbValue ) then
    resultValue = modDbValue;
    compareResult = true; -- used for debugging (remove later)
  end

  GP=F and trace("[EXIT]<%s:%s> resultValue(%s) CompResult(%s)",
    MOD, meth, tostring(resultValue), tostring(compareResult));
  return resultValue;
end -- unTransformSimpleCompare()

-- =======================================================================
-- unTransformComplexCompare()
-- Apply the unTransform function (if not nil) and compare the values,
-- using the objectCompare function (it's a complex compare).
-- Return the unTransformed search value if the values match.
-- parms:
-- (*) ldtMap: Main LDT Control Structure
-- (*) unTransform: The transformation function: Perform if not null
-- (*) dbValue: The value pulled from the DB
-- (*) key: The value we're looking for.
-- =======================================================================
local function
unTransformComplexCompare(ldtMap, unTransform, dbValue, key)
  local meth = "unTransformComplexCompare()";
  GP=E and trace("[ENTER]<%s:%s> storeVal(%s) Key(%s)",
    MOD, meth, tostring(dbValue), tostring(key));

  -- REMEMBER that if we use this mechanism, we'll have to ALSO account
  -- for the FILTER/FARGS mechanism -- which may be out at the caller level,
  -- but it has to be handled.  Right now (8/8/2013) no one is calling
  -- this function.

  local modDbValue = dbValue;
  local resultValue = nil;
  local compareResult;

  if unTransform ~= nil then
    modDbValue = unTransform( dbValue );
  end

  -- If no search key -- then everything matches.
  if( key == nil ) then
    resultValue = modDbValue;
    compareResult = CR_EQUAL;
  else
    compareResult = objectCompare( ldtMap, key, modDbValue );
    if( compareResult == CR_EQUAL ) then
      resultValue = modDbValue;
    end
  end

  GP=F and trace("[EXIT]<%s:%s> resultValue(%s) CompResult(%s)",
    MOD, meth, tostring(resultValue), tostring(compareResult));

  return resultValue;
end -- unTransformComplexCompare()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


-- ======================================================================
-- localInsert( src, topRec, ldtList, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both convertList() and the
-- regular insert().
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The top DB Record:
-- (*) ldtList: The LDT control Structure
-- (*) newValue: Value to be inserted
-- (*) stats: true=Please update Counts, false=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert(src, topRec, ldtList, newValue, stats )
  local meth = "localInsert()";
  GP=E and trace("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));
  local rc = 0;
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- If our state is "compact", do a simple list insert, otherwise do a
  -- real tree insert.
  local insertResult = 0;
  if( ldtMap[R_StoreState] == SS_COMPACT ) then 
    -- Do the COMPACT LIST INSERT
    GP=F and trace("[NOTICE]<%s:%s> Using >>>  LIST INSERT  <<<", MOD, meth);
    local objectList = ldtMap[R_CompactList];
    local key = getKeyValue( ldtMap, newValue );
    local resultMap = searchObjectList( ldtMap, objectList, key );
    if( resultMap.Status == ERR_OK ) then
      -- If FOUND, then we have to verify that Duplicates are allowed.
      -- Otherwise, do the insert.
      if( resultMap.Found == true and ldtMap[R_KeyUnique] == true ) then
        warn("[ERROR]<%s:%s> Unique Key Violation", MOD, meth );
        error( ldte.ERR_UNIQUE_KEY );
      end
      local position = resultMap.Position;
      rc = listInsert( objectList, newValue, position );
      GP=F and trace("[DEBUG]<%s:%s> Insert List rc(%d)", MOD, meth, rc );
      if( rc < 0 ) then
        warn("[ERROR]<%s:%s> Problems with Insert: RC(%d)", MOD, meth, rc );
        error( ldte.ERR_INTERNAL );
      end
    else
      warn("[Internal ERROR]<%s:%s> Key(%s), List(%s)", MOD, meth,
        tostring( key ), tostring( objectList ) );
      error( ldte.ERR_INTERNAL );
    end
  else
    -- Do the TREE INSERT
    GP=F and trace("[NOTICE]<%s:%s> Using >>>  TREE INSERT  <<<", MOD, meth);
    insertResult = treeInsert(src, topRec, ldtList, newValue, stats );
  end

  -- update stats if appropriate.
  if( stats == true and insertResult >= 0 ) then -- Update Stats if success
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
-- getNextLeaf( src, topRec, leafRec  )
-- Our Tree Leaves are doubly linked -- so from any leaf we can move 
-- right or left.  Get the next leaf (right neighbor) in the chain.
-- ======================================================================
local function getNextLeaf( src, topRec, leafRec  )
  local meth = "getNextLeaf()";
  GP=E and trace("[ENTER]<%s:%s> TopRec(%s) src(%s) LeafSummary(%s)",
    MOD, meth, tostring(topRec), tostring(src), leafSummaryString(leafRec));

  local leafRecMap = leafRec[LSR_CTRL_BIN];
  local nextLeafDigest = leafRecMap[LF_NextPage];

  local nextLeaf = nil;
  local nextLeafDigestString;

  if( nextLeafDigest ~= nil and nextLeafDigest ~= 0 ) then
    nextLeafDigestString = tostring( nextLeafDigest );
    GP=F and trace("[OPEN SUB REC]:<%s:%s> Digest(%s)",
      MOD, meth, nextLeafDigestString);

    nextLeaf = openSubrec( src, topRec, nextLeafDigestString )
    if( nextLeaf == nil ) then
      warn("[ERROR]<%s:%s> Can't Open Leaf(%s)",MOD,meth,nextLeafDigestString);
      error( ldte.ERR_SUBREC_OPEN );
    end
  end

  GP=F and trace("[EXIT]<%s:%s> Returning NextLeaf(%s)",
     MOD, meth, leafSummaryString( nextLeaf ) );
  return nextLeaf;

end -- getNextLeaf()

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
-- (*) src: subrecContext
-- (*) topRec
-- (*) ldtBinName
-- (*) ldtList
-- ======================================================================
local function convertList(src, topRec, ldtBinName, ldtList )
  local meth = "convertList()";

  GP=E and trace("[ENTER]<%s:%s>\n\n <><>  CONVERT LIST <><>\n\n", MOD, meth );
  GP=E and trace("[ENTER]<%s:%s>\n\n <><>  CONVERT LIST <><>\n\n", MOD, meth );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];
  local binName = propMap[PM_BinName];

  -- iterate thru the ldtMap CompactList, re-inserting each item.
  local compactList = ldtMap[R_CompactList];

  if compactList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
      MOD, meth, tostring(singleBinName));
    error( ldte.ERR_INTERNAL );
  end

  ldtMap[R_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode

  -- Rebuild. Take the compact list and insert it into the tree.
  -- The good way to do it is to sort the items and put them into a leaf
  -- in sorted order.  The simple way is to insert each one into the tree.
  -- Start with the SIMPLE way.
  -- TODO: @TOBY: Change this to build the tree in one operation.
  for i = 1, list.size( compactList ), 1 do
    -- Do NOT update counts, as we're just RE-INSERTING existing values.
    treeInsert( src, topRec, ldtList, compactList[i], false );
  end

  -- Now, release the compact list we were using.
  -- TODO: Figure out exactly how Lua releases storage
  -- ldtMap[R_CompactList] = nil; -- Release the list.  Does this work??
  ldtMap[R_CompactList] = list();  -- Replace with an empty list.

  GP=F and trace("[EXIT]: <%s:%s> ldtSummary(%s)",
    MOD, meth, tostring(ldtList));
  return 0;
end -- convertList()

-- ======================================================================
-- Given the searchPath result from treeSearch(), Scan the leaves for all
-- values that satisfy the searchPredicate and the filter.
-- ======================================================================
local function 
treeScan(src, resultList, topRec, sp, ldtList, key, func, fargs )
  local meth = "treeScan()";
  local rc = 0;
  local scan_A = 0;
  local scan_B = 0;
  GP=E and trace("[ENTER]<%s:%s> searchPath(%s) key(%s)",
      MOD, meth, tostring(sp), tostring(key) );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  local leafLevel = sp.LevelCount;
  local leafRec = sp.RecList[leafLevel];

  local count = 0;
  local done = false;
  local startPosition = sp.PositionList[leafLevel];
  while not done do
    GP=F and trace("[LOOP DEBUG]<%s:%s>Loop Top: Count(%d)", MOD, meth, count );
    -- NOTE: scanLeaf() actually returns a "double" value -- the first is
    -- the scan instruction (stop=0, continue=1) and the second is the error
    -- return code.  So, if scan_B is "ok" (0), then we look to scan_A to see
    -- if we should continue the scan.
    scan_A, scan_B  = scanLeaf(topRec, leafRec, startPosition, ldtMap,
                              resultList, key, func, fargs, flag)

-- Uncomment this line to see the "LEAF BOUNDARIES" in the data.
-- It's purely for debugging
-- list.append(resultList, 999999 );

    -- Look and see if there's more scanning needed. If so, we'll read
    -- the next leaf in the tree and scan another leaf.
    if( scan_B < 0 ) then
      warn("[ERROR]<%s:%s> Problems in ScanLeaf() A(%s) B(%s)",
        MOD, meth, tostring( scan_A ), tostring( scan_B ) );
      error( ldte.ERR_INTERNAL );
    end
      
    if( scan_A == SCAN_CONTINUE ) then
      GP=F and trace("[STILL SCANNING]<%s:%s>", MOD, meth );
      startPosition = 1; -- start of next leaf
      leafRec = getNextLeaf( src, topRec, leafRec );
      if( leafRec == nil ) then
        GP=F and trace("[NEXT LEAF RETURNS NIL]<%s:%s>", MOD, meth );
        done = true;
      end
    else
      GP=F and trace("[DONE SCANNING]<%s:%s>", MOD, meth );
      done = true;
    end
  end -- while not done reading the T-leaves

  GP=F and trace("[EXIT]<%s:%s>SearchKey(%s) ResultList(%s) SearchPath(%s)",
      MOD,meth,tostring(key),tostring(resultList),tostring(sp));

  return rc;

end -- treeScan()

-- ======================================================================
-- listDelete()
-- ======================================================================
-- General List Delete function that can be used to delete items, employees
-- or pesky Indian Developers (usually named "Raj").
-- RETURN:
-- A NEW LIST that 
-- ======================================================================
local function listDelete( objectList, key, position )
  local meth = "listDelete()";
  local resultList;
  local listSize = list.size( objectList );

  GP=E and trace("[ENTER]<%s:%s>List(%s) size(%d) Key(%s) Position(%d)", MOD,
  meth, tostring(objectList), listSize, tostring(key), position );
  
  if( position < 1 or position > listSize ) then
    warn("[DELETE ERROR]<%s:%s> Bad position(%d) for delete: key(%s)",
      MOD, meth, position, tostring(key));
    error( ldte.ERR_DELETE );
  end

  -- Move elements in the list to "cover" the item at Position.
  --  +---+---+---+---+
  --  |111|222|333|444|   Delete item (333) at position 3.
  --  +---+---+---+---+
  --  Moving forward, Iterate:  list[pos] = list[pos+1]
  --  This is what you would THINK would work:
  -- for i = position, (listSize - 1), 1 do
  --   objectList[i] = objectList[i+1];
  -- end -- for()
  -- objectList[i+1] = nil;  (or, call trim() )
  -- However, because we cannot assign "nil" to a list, nor can we just
  -- trim a list, we have to build a NEW list from the old list, that
  -- contains JUST the pieces we want.
  -- So, basically, we're going to build a new list out of the LEFT and
  -- RIGHT pieces of the original list.
  --
  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- The special cases are:
  -- (*) A list of size 1:  Just return a new (empty) list.
  -- (*) We're deleting the FIRST element, so just use RIGHT LIST.
  -- (*) We're deleting the LAST element, so just use LEFT LIST
  if( listSize == 1 ) then
    resultList = list();
  elseif( position == 1 ) then
    resultList = list.drop( objectList, 1 );
  elseif( position == listSize ) then
    resultList = list.take( objectList, position - 1 );
  else
    resultList = list.take( objectList, position - 1);
    local addList = list.drop( objectList, position );
    local addLength = list.size( addList );
    for i = 1, addLength, 1 do
      list.append( resultList, addList[i] );
    end
  end

  -- When we do deletes with Dups -- we'll change this to have a 
  -- START position and an END position (or a length), rather than
  -- an assumed SINGLE cell.
  warn("[WARNING!!!]: Currently performing ONLY single delete");

  GP=F and trace("[EXIT]<%s:%s>List(%s)", MOD, meth, tostring(resultList));
  return resultList;
end -- listDelete()


-- ======================================================================
-- leafDelete()
-- ======================================================================
-- Collapse the list to get rid of the entry in the leaf.
-- We're not yet in the mode of "NULLing" out the entry, so we'll pay
-- the extra cost of collapsing the list around the item.  The SearchPath
-- parm shows us where the item is.
-- Parms: 
-- (*) src: SubRec Context (in case we have to open more leaves)
-- (*) sp: Search Path structure
-- (*) topRec:
-- (*) ldtList:
-- (*) key: the key -- in case we need to look for more dups
-- ======================================================================
local function leafDelete( src, sp, topRec, ldtList, key )
  local meth = "leafDelete()";
  GP=E and trace("[ENTER]<%s:%s> LDT(%s) key(%s)", MOD, meth,
    ldtSummaryString( ldtList ), tostring( key ));
  local rc = 0;

  -- Our list and map has already been validated.  Just use it.
  propMap = ldtList[1];
  ldtMap  = ldtList[2];

  local leafLevel = sp.LevelCount;
  local leafRec = sp.RecList[leafLevel];
  local objectList = leafRec[LSR_LIST_BIN];
  local position = sp.PositionList[leafLevel];
  
  GP=F and trace("[DUMP]Before delete(%s) Key(%s) Position(%d)",
    tostring(objectList), tostring(key), position);

  local resultList = listDelete( objectList, key, position )
  leafRec[LSR_LIST_BIN] = resultList;

  GP=F and trace("[DUMP]After delete(%s) Key(%s)", tostring(resultList), tostring(key));

  rc = aerospike:update_subrec( leafRec );
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>::Updating TopRec", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) newValue(%s) rc(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue), tostring(rc));
  return rc;
end -- leafDelete()

-- ======================================================================
-- treeDelete()
-- ======================================================================
-- Perform the delete of the delete value.  Remove this object from the
-- tree.  Two cases:
-- (*) Unique Key
-- (*) Duplicates Allowed.
-- We'll start with unique key -- just collapse the object list in the 
-- leaf to remove the item.
-- When we do Duplicates, then we have to address the case that the leaf
-- is completely empty, which means we also need remove the subrec from
-- the leaf chain.  HOWEVER, for now, we'll just remove the items from the
-- leaf objectList, but leave the Tree Leaves in place.  And, in either
-- case, we won't update the upper nodes.
-- We will have both a COMPACT storage mode and a TREE storage mode. 
-- When in COMPACT mode, the root node holds the list directly (linear
-- search and delete).  When in Tree mode, the root node holds the top
-- level of the tree.
-- Parms:
-- (*) src: SubRec Context
-- (*) topRec:
-- (*) ldtList: The LDT Control Structure
-- (*) key:  Find and Delete the objects that match this key
-- (*) createSpec:
-- Return:
-- ERR_OK(0): if found
-- ERR_NOT_FOUND(-2): if NOT found
-- ERR_GENERAL(-1): For any other error 
-- =======================================================================
local function treeDelete( src, topRec, ldtList, key )
  local meth = "treeDelete()";
  GP=E and trace("[ENTER]<%s:%s> LDT(%s) key(%s)", MOD, meth,
    ldtSummaryString( ldtList ), tostring( key ));
  local rc = 0;

  -- Our list and map has already been validated.  Just use it.
  propMap = ldtList[1];
  ldtMap  = ldtList[2];

  local sp = createSearchPath(ldtMap);
  local status = treeSearch( src, topRec, sp, ldtList, key );

  if( status == ST_FOUND ) then
    rc = leafDelete( src, sp, topRec, ldtList, key );
    if( rc == 0 ) then
      rc = closeAllSubrecs( src );
      if( rc < 0 ) then
        warn("[ERROR]<%s:%s> Problems in closeAllSubrecs() SRC(%s)",
          MOD, meth, tostring( src ));
        error( ldte.ERR_SUBREC_CLOSE );
      end
    end
  else
    rc = ERR_NOT_FOUND;
  end

  -- All of the subrecords were written out in the respective insert methods,
  -- so if all went well, we'll now update the top record. Otherwise, we
  -- will NOT udate it.
  if( rc == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>::Updating TopRec", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]<%s:%s>LdtSummary(%s) newValue(%s) rc(%s)",
    MOD, meth, ldtSummaryString(ldtList), tostring(newValue), tostring(rc));
  return rc;
end -- treeDelete()

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

  GP=F and trace("\n\n >>>>>>>>> API[ LLIST CREATE ] <<<<<<<<<< \n\n");

  if argList == nil then
    GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL argList",
      MOD, meth, tostring(ldtBinName));
  else
    GP=E and trace("[ENTER2]: <%s:%s> ldtBinName(%s) argList(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( argList ));
  end

  -- Validate the BinName -- this will kick out if there's anything wrong
  -- with the bin name.
  validateBinName( ldtBinName );

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error
  if topRec[ldtBinName] ~= nil  then
    warn("[ERROR EXIT]: <%s:%s> LDT BIN(%s) Already Exists",
      MOD, meth, tostring(ldtBinName) );
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end

  -- Create and initialize the LDT MAP -- the main LDT structure
  -- initializeLList() also assigns the map to the record bin.
  local ldtList = initializeLList( topRec, ldtBinName );
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
  local rc;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end
  
  -- Process Create/Update results.
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=F and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end
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
  GP=E and trace("[ENTER]<%s:%s>LLIST BIN(%s) NwVal(%s) createSpec(%s)",
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
    ldtList = initializeLList( topRec, ldtBinName );
    propMap = ldtList[1];
    ldtMap  = ldtList[2];
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLListMap( ldtMap, createSpec ); -- Map, not list, used here
    end
    topRec[ldtBinName] = ldtList;
  else
    -- all there, just use it
    ldtList = topRec[ ldtBinName ];
    propMap = ldtList[1];
    ldtMap  = ldtList[2];
  end
  -- Note: We'll do the aerospike:create() at the end of this function,
  -- if needed.
  -- DESIGN NOTE: All "outer" functions, like this one, will create a
  -- "subrecContext" object, which will hold all of the open subrecords.
  -- The key will be the DigestString, and the value will be the subRec
  -- pointer.  At the end of the call, we will iterate thru the subrec
  -- context and close all open subrecords.  Note that we may also need
  -- to mark them dirty -- but for now we'll update them in place (as needed),
  -- but we won't close them until the end.
  -- This is needed for both the "convertList()" call, which makes multiple
  -- calls to the treeInsert() function (which opens and closes subrecs) and
  -- the regular treeInsert() call, which, in the case of a split, may do
  -- a lot of opens/closes of nodes and leaves.
  local src = createSubrecContext();

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to turn our single list into a tree.
  local totalCount = ldtMap[R_TotalCount];
  GP=F and trace("[NOTICE!!]<%s:%s>Checking State for Conversion", MOD, meth );
  GP=F and trace("[NOTICE!!]<%s:%s>State(%s) C val(%s) TotalCount(%d)", MOD,
    meth, tostring(ldtMap[R_StoreState]), tostring(SS_COMPACT), totalCount);

  -- We're going to base the conversion on TotalCount, not ItemCount, since
  -- it's really the amount of space we're using (empty slots and full slots)
  -- not just the full slots (which would be ItemCount).
  if(( ldtMap[R_StoreState] == SS_COMPACT ) and
     ( totalCount >= ldtMap[R_Threshold] )) 
  then
    convertList(src, topRec, ldtBinName, ldtList );
  end
 
  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert(src, topRec, ldtList, newValue, true );

  -- This is a debug "Tree Print" 
  -- GP=F and printTree( src, topRec, ldtBinName );

  -- Close ALL of the subrecs that might have been opened
  rc = closeAllSubrecs( src );

  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems in closeAllSubrecs() SRC(%s)",
      MOD, meth, tostring( src ));
    error( ldte.ERR_SUBREC_CLOSE );
  end

  -- All done, store the record (either CREATE or UPDATE)
  local rc;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create TopRecord()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update TopRecord()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  -- Process Create/Update results.
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=F and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end

end -- function localLListInsert()

-- =======================================================================
-- List Insert -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function llist_insert( topRec, ldtBinName, newValue )
  GP=F and trace("\n\n >>>>>>>>> API[ LLIST INSERT ] <<<<<<<<<(%s) \n",
    tostring(newValue));
  return localLListInsert( topRec, ldtBinName, newValue, nil )
end -- end llist_insert()

function llist_create_and_insert( topRec, ldtBinName, newValue, createSpec )
  GP=F and trace("\n\n >>>>>>> API[ LLIST CREATE And INSERT ] <<<<<<<<(%s)\n",
    tostring(newValue));
  return localLListInsert( topRec, ldtBinName, newValue, createSpec );
end -- llist_create_and_insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || localLListSearch:
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return all of the objects that match "key".
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) key
-- (*) func:
-- (*) fargs:
-- ======================================================================
local function localLListSearch( topRec, ldtBinName, key, func, fargs )
  local meth = "localLListSearch()";
  GP=E and trace("[ENTER]<%s:%s> bin(%s) key(%s) ", MOD, meth,
      tostring( ldtBinName), tostring(key) );

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

  -- Create our subrecContext, which tracks all open SubRecords during
  -- the call.  Then, allows us to close them all at the end.
  local src = createSubrecContext();

  local resultA;
  local resultB;

  -- If our state is "compact", do a simple list search, otherwise do a
  -- full tree search.
  if( ldtMap[R_StoreState] == SS_COMPACT ) then 
    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- Do the COMPACT LIST SEARCH
    local objectList = ldtMap[R_CompactList];
    local resultMap = searchObjectList( ldtMap, objectList, key );
    if( resultMap.Status == ERR_OK and resultMap.Found == true ) then
      local position = resultMap.Position;
      resultA, resultB =  listScan(objectList, position, ldtMap,
                    resultList, key, func, fargs, flag);
      GP=F and trace("[DEBUG]<%s:%s> Scan Compact List:Res(%s) A(%s) B(%s)",
        MOD, meth, tostring(resultList), tostring(resultA), tostring(resultB));
      if( resultB < 0 ) then
        warn("[ERROR]<%s:%s> Problems with Scan: Key(%s), List(%s)", MOD, meth,
          tostring( key ), tostring( objectList ) );
        error( ldte.ERR_INTERNAL );
      end
    else
      warn("[ERROR]<%s:%s> Search Not Found: Key(%s), List(%s)", MOD, meth,
        tostring( key ), tostring( objectList ) );
      error( ldte.ERR_NOT_FOUND );
    end
    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  else
    -- Do the TREE Search
    GP=F and trace("[DEBUG]<%s:%s> Searching Tree", MOD, meth );
    local sp = createSearchPath(ldtMap);
    rc = treeSearch( src, topRec, sp, ldtList, key );
    if( rc == ST_FOUND ) then
      rc = treeScan( src, resultList, topRec, sp, ldtList, key, func, fargs );
      if( rc < 0 or list.size( resultList ) == 0 ) then
          warn("[ERROR]<%s:%s> Tree Scan Problem: RC(%d) after a good search",
            MOD, meth, rc );
      end
    else
      warn("[ERROR]<%s:%s> Tree Search Not Found: Key(%s)", MOD, meth,
        tostring( key ) );
      error( ldte.ERR_NOT_FOUND );
    end
  end -- tree search

  -- Close ALL of the subrecs that might have been opened
  rc = closeAllSubrecs( src );
  if( rc < 0 ) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  GP=F and trace("[EXIT]: <%s:%s>: Search Key(%s) Returns (%s)",
  MOD, meth, tostring(key), tostring(resultList));
  
  -- We have either jumped out of here via error() function call, or if
  -- we got this far, then we are supposed to have a valid resultList.
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
function llist_search( topRec, ldtBinName, searchKey )
  local meth = "listSearch()";
  GP=E and trace("[ENTER]<%s:%s> LLIST BIN(%s) searchKey(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchKey) )

  GP=F and trace("\n\n >>>>>>>>> API[ LLIST SEARCH ] <<<<<<<<<(%s) \n",
    tostring(searchKey));

  return localLListSearch( topRec, ldtBinName, searchKey, nil, nil );
end -- end llist_search()

function llist_search_with_filter(topRec,ldtBinName,searchKey,func,fargs )
  local meth = "listSearch()";
  GP=E and trace("[ENTER]<%s:%s> BIN(%s) searchKey(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchKey),
    tostring(func), tostring(fargs));

  GP=F and trace("\n\n >>>>>>>> API[ LLIST SEARCH With FILTER] <<<<<<<(%s)\n",
    tostring(searchKey));

  return localLListSearch( topRec, ldtBinName, searchKey, func, fargs );
end -- end llist_search_with_filter()

-- =======================================================================
-- llist_scan -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type.
-- NOTE: After a bit of thought -- we don't need a separate internal
-- scan function.  Search with a nil searchKey works just fine (I think).
-- =======================================================================
function llist_scan( topRec, ldtBinName )
  local meth = "llist_scan()";
  GP=E and trace("[ENTER]<%s:%s> LLIST BIN(%s)",
    MOD, meth, tostring(ldtBinName) );

  GP=F and trace("\n\n  >>>>>>>> API[ SCAN ] <<<<<<<<<<<<<<<<<< \n");

  return localLListSearch( topRec, ldtBinName, nil, nil, nil );
end -- end llist_scan()

function llist_scan_with_filter( topRec, ldtBinName, func, fargs )
  local meth = "llist_scan_with_filter()";
  GP=E and trace("[ENTER]<%s:%s> BIN(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(func), tostring(fargs));

  GP=F and trace("\n\n  >>>>>>>>> API[ SCAN and FILTER ]<<<<<<<< \n\n");

  return localLListSearch( topRec, ldtBinName, nil, func, fargs );
end -- end llist_scan()

-- ======================================================================
-- || llist_delete ||
-- ======================================================================
-- Delete the specified item(s).
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) LdtBinName
-- (3) key: The key we'll search for
--
function llist_delete( topRec, binName, key )
  local meth = "llist_delete()";
  local rc = 0;

  GP=F and trace("\n\n  >>>>>>> API[ DELETE ] <<<<<<<<<<<<<<<(%s) \n",
    tostring(key));

  GP=E and trace("[ENTER]<%s:%s>ldtBinName(%s) key(%s)",
      MOD, meth, tostring(binName), tostring(key));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, binName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  ldtList = topRec[ binName ];
  local propMap = ldtList[1];
  local ldtMap  = ldtList[2];

  -- Create our subrecContext, which tracks all open SubRecords during
  -- the call.  Then, allows us to close them all at the end.
  local src = createSubrecContext();

  -- If our state is "compact", do a simple list insert, otherwise do a
  -- real tree insert.
  local insertResult = 0;
  if( ldtMap[R_StoreState] == SS_COMPACT ) then 
    -- Search the compact list, find the location, then delete it.
    GP=F and trace("[NOTICE]<%s:%s> Using COMPACT DELETE", MOD, meth);
    local objectList = ldtMap[R_CompactList];
    resultMap = searchObjectList( ldtMap, objectList, key );
    if( resultMap.Status == ERR_OK and resultMap.Found == true ) then
      ldtMap[R_CompactList] = listDelete(objectList, key, resultMap.Position);
    else
      error( ldte.ERR_NOT_FOUND );
    end
  else
    GP=F and trace("[NOTICE]<%s:%s> Using >>>  TREE DELETE  <<<", MOD, meth);
    rc = treeDelete(src, topRec, ldtList, key );
  end

  -- update stats if successful
  if( rc >= 0 ) then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = ldtMap[R_TotalCount];
    propMap[PM_ItemCount] = itemCount - 1; 
    ldtMap[R_TotalCount] = totalCount - 1;
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", MOD, meth, itemCount );
  end
  topRec[ binName ] = ldtList;

  -- Validate results -- if anything bad happened, then the record
  -- probably did not change -- we don't need to udpate.
  if( rc == 0 ) then
    -- Close ALL of the subrecs that might have been opened
    rc = closeAllSubrecs( src );
    if( rc < 0 ) then
      warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
      error( ldte.ERR_SUBREC_CLOSE );
    end

    -- All done, store the record
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );

    -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
    -- so just turn any NILs into zeros.
    rc = aerospike:update( topRec );
    if( rc == nil or rc == 0 ) then
      GP=F and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
      return 0;
    else
      GP=F and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
      error( ldte.ERR_INTERNAL );
    end
  else
    GP=F and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_DELETE );
  end
end -- function llist_delete()

-- ========================================================================
-- ldtRemove() -- Remove the LDT entirely from the record.
-- NOTE: This could eventually be moved to COMMON, and be "ldt_remove()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Question  -- Reset the record[binName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) binName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function ldtRemove( topRec, binName )
  local meth = "ldtRemove()";

  GP=E and trace("[ENTER]: <%s:%s> binName(%s)",
    MOD, meth, tostring(binName));
  local rc = 0; -- start off optimistic

  -- Validate the binName before moving forward
  validateRecBinAndMap( topRec, binName, true );

  -- Extract the property map and lso control map from the lso bin list.
  local ldtList = topRec[ binName ];
  local propMap = ldtList[1];

  GP=F and trace("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), ldtSummaryString( ldtList ));

  -- Get the ESR and delete it.
  local esrDigest = propMap[PM_EsrDigest];
  local esrDigestString = tostring(esrDigest);
  local esrRec = aerospike:open_subrec( topRec, esrDigestString );
  GP=F and trace("[STATUS]<%s:%s> About to Call Aerospike REMOVE", MOD, meth );
  rc = aerospike:remove_subrec( esrRec );
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
  else
    warn("[ESR DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, binName);
    error( ldte.ERR_SUBREC_DELETE );
  end

  topRec[binName] = nil;

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
      MOD, meth );
    error( ldte.ERR_BIN_DAMAGED );
  end
  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- Remove this bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
  end
  
  -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
  -- so just turn any NILs into zeros.
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=F and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=F and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end

end -- ldtRemove()

-- ========================================================================
-- llist_remove() -- Remove the LDT entirely from the record.
-- NOTE: This could eventually be moved to COMMON, and be "ldt_remove()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Question  -- Reset the record[lsoBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function llist_remove( topRec, lsoBinName )
  GP=F and trace("\n\n >>>>>>>>> API[ LLIST REMOVE ] Bin(%s) <<<<<<<<<<\n",
    lsoBinName );
  return ldtRemove( topRec, lsoBinName );
end -- llist_remove()

-- ========================================================================
-- llist_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function llist_size( topRec, ldtBinName )
  local meth = "llist_size()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  GP=F and trace("\n\n >>>>>>>>> API[ LLIST SIZE ] <<<<<<<<<(%s)\n",ldtBinName);

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

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  GP=F and trace("\n\n >>>>>>>>> API[ LLIST CONFIG ] <<<<<<<<(%s)\n",ldtBinName);

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local config = ldtSummary( topRec[ ldtBinName ] );

  GP=F and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, config );

  return config;
end -- function llist_config()

-- ========================================================================
-- Debugging/Tracing mechanism -- show the WHOLE tree.
-- ========================================================================
function ldt_dump( topRec, ldtBinName )
  GP=F and trace("\n\n >>>>>>>>> API[ LLIST DUMP ] <<<<<<<<(%s)\n",ldtBinName);
  local src = createSubrecContext();
  printTree( src, topRec, ldtBinName );
  return 0;
end -- function ldt_dump()

-- ========================================================================
-- llist_debug() -- Turn the debug setting on (1) or off (0)
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function llist_debug( topRec, setting )
  local meth = "llist_debug()";
  local rc = 0;

  GP=E and trace("[ENTER]: <%s:%s> setting(%s)", MOD, meth, tostring(setting));
  if( setting ~= nil and type(setting) == "number" ) then
    if( setting == 1 ) then
      trace("[DEBUG SET]<%s:%s> Turn Debug ON(%s)", MOD, meth );
      F = true;
    elseif( setting == 0 ) then
      trace("[DEBUG SET]<%s:%s> Turn Debug OFF(%s)", MOD, meth );
      F = false;
    else
      trace("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
      rc = -1;
    end
  else
    trace("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
    rc = -1;
  end
  return rc;
end -- llist_debug()


-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
