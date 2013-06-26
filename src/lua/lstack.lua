-- Large Stack Object (LSO or LSTACK) Operations
-- lstack.lua:  June 26, 2013
--
-- Module Marker: Keep this in sync with the stated version
local MOD="lstack_2013_06_26.9"; -- the module name used for tracing

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.2;

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true; -- Leave this ALWAYS true (but value seems not to matter)
local F=true; -- Set F (flag) to true to turn ON global print

-- ======================================================================
-- Additional lstack documentation may be found in: lstack_design.lua.
-- ======================================================================
-- LSTACK Design and Type Comments (aka Large Stack Object, or LSO).
-- The lstack type is a member of the new Aerospike Large Type family,
-- Large Data Types (LDTs).  LDTs exist only on the server, and thus must
-- undergo some form of translation when passing between client and server.
--
-- LSTACK is a server side type that can be manipulated ONLY by this file,
-- lstack.lua.  We prevent any other direct manipulation -- any other program
-- or process must use the lstack api provided by this program in order to
--
-- An LSTACK value -- stored in a record bin -- is represented by a Lua MAP
-- object that comprises control information, a directory of records
-- (for "warm data") and a "Cold List Head" ptr to a linked list of directory
-- structures that each point to the records that hold the actual data values.
--
-- LSTACK Functions Supported (Note switch to lower case)
-- (*) lstack_create: Create the LSO structure in the chosen topRec bin
-- (*) lstack_push: Push a user value (AS_VAL) onto the stack
-- (*) lstack_create_and_push: Push a user value (AS_VAL) onto the stack
-- (*) lstack_peek: Read N values from the stack, in LIFO order
-- (*) lstack_peek_then_filter: Read N values from the stack, in LIFO order
-- (*) lstack_trim: Release all but the top N values.
-- (*) lstack_delete: Release all storage related to this lstack object
-- (*) lstack_config: retrieve all current config settings in map format
-- (*) lstack_size: Report the NUMBER OF ITEMS in the stack.
-- (*) lstack_subrec_list: Return the list of subrec digests
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large Stack Object (LSO) is
-- referred to as an "LSO" bin. The overhead of the LSO value is 
-- (*) LSO Control Info (~70 bytes)
-- (*) LSO Hot Cache: List of data entries (on the order of 100)
-- (*) LSO Warm Directory: List of Aerospike Record digests:
--     100 digests(250 bytes)
-- (*) LSO Cold Directory Head (digest of Head plus count) (30 bytes)
-- (*) Total LSO Record overhead is on the order of 350 bytes
-- NOTES:
-- (*) In the Hot Cache, the data items are stored directly in the
--     cache list (regardless of whether they are bytes or other as_val types)
-- (*) In the Warm Dir List, the list contains aerospike digests of the
--     LSO Data Records (LDRs) that hold the Warm Data.  The LDRs are
--     opened (using the digest), then read/written, then closed/updated.
-- (*) The Cold Dir Head holds the Aerospike Record digest of a record that
--     holds a linked list of cold directories.  Each cold directory holds
--     a list of digests that are the cold LSO Data Records.
-- (*) The Warm and Cold LSO Data Records use the same format -- so they
--     simply transfer from the warm list to the cold list by moving the
--     corresponding digest from the warm list to the cold list.
-- (*) Record types used in this design:
-- (1) There is the main record that contains the LSO bin (LSO Head)
-- (2) There are LSO Data "Chunk" Records (both Warm and Cold)
--     ==> Warm and Cold LSO Data Records have the same format:
--         They both hold User Stack Data.
-- (3) There are Chunk Directory Records (used in the cold list)
--
-- (*) How it all connects together....
-- (+) The main record points to:
--     - Warm Data Chunk Records (these records hold stack data)
--     - Cold Data Directory Records (these records hold ptrs to Cold Chunks)
--
-- (*) We may have to add some auxilliary information that will help
--     pick up the pieces in the event of a network/replica problem, where
--     some things have fallen on the floor.  There might be some "shadow
--     values" in there that show old/new values -- like when we install
--     a new cold dir head, and other things.  TBD
--
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |o o o|LSO  |                                        |
-- |Bin 1|Bin 2|o o o|Bin 1|                                        |
-- +-----+-----+-----+-----+----------------------------------------+
--                  /       \                                       
--   ================================================================
--     LSO Map                                              
--     +-------------------+                                 
--     | LSO Control Info  | About 20 different values kept in Ctrl Info
--     |...................|
--     |...................|< Oldest ................... Newest>            
--     +-------------------+========+========+=======+=========+
--     |<Hot Entry Cache>  | Entry 1| Entry 2| o o o | Entry n |
--     +-------------------+========+========+=======+=========+
--     |...................|HotCache entries are stored directly in the record
--     |...................| 
--     |...................|WarmCache Digests are stored directly in the record
--     |...................|< Oldest ................... Newest>            
--     +-------------------+========+========+=======+=========+
--     |<Warm Digest List> |Digest 1|Digest 2| o o o | Digest n|
--     +-------------------+===v====+===v====+=======+====v====+
--  +-<@>Cold Dir List Head|   |        |                 |    
--  |  +-------------------+   |        |                 |    
--  |                    +-----+    +---+      +----------+   
--  |                    |          |          |     Warm Data(WD)
--  |                    |          |          |      WD Rec N
--  |                    |          |          +---=>+--------+
--  |                    |          |     WD Rec 2   |Entry 1 |
--  |                    |          +---=>+--------+ |Entry 2 |
--  |                    |      WD Rec 1  |Entry 1 | |   o    |
--  |                    +---=>+--------+ |Entry 2 | |   o    |
--  |                          |Entry 1 | |   o    | |   o    |
--  |                          |Entry 2 | |   o    | |Entry n |
--  |                          |   o    | |   o    | +--------+
--  |                          |   o    | |Entry n |
--  |                          |   o    | +--------+
--  |                          |Entry n | "LDR" (LSO Data Record) Pages
--  |                          +--------+ [Warm Data (LDR) Chunks]
--  |                                            
--  |                           <Newest Dir............Oldest Dir>
--  +-------------------------->+-----+->+-----+->+-----+ ->+-----+
--                              |Rec  |  |Rec  |  |Rec  | o |Rec  |
--    The cold dir is a linked  |Chunk|  |Chunk|  |Chunk| o |Chunk|
--    list of dir pages that    |Dir  |  |Dir  |  |Rec  | o |Dir  |
--    point to LSO Data Records +-----+  +-----+  +-----+   +-----+
--    that hold the actual cold [][]:[]  [][]:[]  [][]:[]   [][]:[]
--    data (cold chunks).       +-----+  +-----+  +-----+   +-----+
--                               | |  |   | |  |   | |  |    | |  |
--    LDRS (per dir) have age:   | |  V   | |  V   | |  V    | |  V
--    <Oldest LDR .. Newest LDR> | | :+--+| | :+--+| | :+--+ | | :+--+
--    As "Warm Data" ages out    | | :|Cn|| | :|Cn|| | :|Cn| | | :|Cn|
--    of the Warm Dir List, the  | V :+--+| V :+--+| V :+--+ | V :+--+
--    LDRs transfer out of the   | +--+   | +--+   | +--+   | +--+
--    Warm Directory and into    | |C2|   | |C2|   | |C2|   | |C2|
--    the cold directory.        V +--+   V +--+   V +--+   V +--+
--                               +--+     +--+     +--+      +--+
--    The Warm and Cold LDRs     |C1|     |C1|     |C1|      |C1|
--    have identical structure.  +--+     +--+     +--+      +--+
--                                A        A        A         A    
--                                |        |        |         |
--     [Cold Data (LDR) Chunks]---+--------+--------+---------+
--
--
-- The "Hot Entry Cache" is the true "Top of Stack", holding roughly the
-- top 50 to 100 values.  The next level of storage is found in the first
-- Warm dir list (the last Chunk in the list).  Since we process stack
-- operations in LIFO order, but manage them physically as a list
-- (append to the end), we basically read the pieces in top down order,
-- but we read the CONTENTS of those pieces backwards.  It is too expensive
-- to "prepend" to a list -- and we are smart enough to figure out how to
-- read an individual page list bottom up (in reverse append order).
--
-- We don't "age" the individual entries out one at a time as the Hot Cache
-- overflows -- we instead take a group at a time (specified by the
-- HotCacheTransferAmount), which opens up a block of empty spots. Notice that
-- the transfer amount is a tuneable parameter -- for heavy reads, we would
-- want MORE data in the cache, and for heavy writes we would want less.
--
-- If we generally pick half (e.g. 100 entries total, and then transfer 50 at
-- a time when the cache fills up), then half the time the inserts will affect
-- ONLY the Top (LSO) record -- so we'll have only one Read, One Write 
-- operation for a stack push.  1 out of 50 will have the double read,
-- double write, and 1 out of 10,000 (or so) will have additional
-- IO's depending on the state of the Warm/Cold lists.
-- Notice ALSO that when we use a coupled Namespace for LDTs (main memory
-- for the top records and SSD for the subrecords), then 49 out of 50
-- writes and small reads will have ZERO I/O cost -- since it will be
-- contained in the main memory record.
--
-- NOTE: Design, V3.x.  For really cold data -- things out beyond 50,000
-- elements, it might make sense to just push those out to a real disk
-- based file (to which we could just append -- and read in reverse order).
-- If we ever need to read the whole stack, we can afford
-- the time and effort to read the file (it is an unlikely event).  The
-- issue here is that we probably have to teach Aerospike how to transfer
-- (and replicate) files as well as records.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- REMEMBER THAT ALL INSERTS ARE INTO HOT LIST -- and transforms are done
-- there.  All UNTRANSFORMS are done reading from the List (Hot List or
-- warm/cold Data Page List).  Notice that even though the values may be
-- transformed (compacted into) bytes, they are still just inserted into
-- the hot list, we don't try to pack them into an array;
-- that is done only in the warm/cold pages (where the benefit is greater).
-- 
-- Read Filters are applied AFTER the UnTransform (bytes and list).
--
-- NOTE: New changes with V4.3 to Push and Peek.
-- (*) Stack Push has an IMPLICIT transform function -- which is defined
--     in the create spec.  So, the two flavors of Stack Push are now
--     + lstack_push(): with implicit transform when defined
--     + lstack_create_and_push(): with the ability to create as
--       needed -- and with the supplied create_spec parameter.
-- (*) Stack Peek has an IMPLICIT UnTransform function -- which is defined
--     in the create spec.  So, the two flavors of Stack Peek are now
--     + lstack_peek(): with implicit untransform, when defined in create.
--     + lstack_peek_then_filter(): with implicit untransform and a filter
--       to act as an additional query mechanism.
--
-- On Create, a Large Stack Object can be configured with a Transform function,
-- to be used on storage (push) and an UnTransform function, to be used on
-- retrieval (peek).
-- (*) stack_push(): Push a user value (AS_VAL) onto the stack, 
--     calling the Transform on the value FIRST to transform it before
--     storing it on the stack.
-- (*) stack_peek_then_filter: Retrieve N values from the stack, and for each
--     value, apply the transformation/filter UDF to the value before
--     adding it to the result list.  If the value doesn't pass the
--     filter, the filter returns nil, and thus it would not be added
--     to the result list.
-- ======================================================================
-- TO DO List: for Future (once delete_subrec() is available)
-- TODO: Implement stack_trim(): Must release storage before record delete.
-- TODO: Implement stack_delete(): Release all storage for this LDT
-- TODO: Implement LStackSubRecordDestructor():
-- TODO: Add Exists Subrec Digest in LsoMap.
-- ======================================================================
-- Aerospike SubRecord Calls:
-- newRec = aerospike:create_subrec( topRec )
-- newRec = aerospike:open_subrec( topRec, childRecDigest)
-- status = aerospike:update_subrec( childRec )
-- status = aerospike:close_subrec( childRec )
-- status = aerospike:delete_subrec( topRec, childRec ) (not yet ready)
-- digest = record.digest( childRec )
-- status = record.set_type( topRec, recType )
-- status = record.set_flags( topRec, binName, binFlags )
-- ======================================================================
-- For additional Documentation, please see lstack_design.lua, which should
-- be co-located in the main Development tree with lstack.lua
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Table of Functions: Used for Transformation and Filter Functions.
-- This is held in UdfFunctionTable.lua.  Look there for details.
-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
local MAGIC="MAGIC";     -- the magic value for Testing LSO integrity

-- Get addressability to the Function Table: Used for compress and filter
local functionTable = require('UdfFunctionTable');

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

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

-- Bin Flag Types
local BF_LDT_BIN     = 1; -- Main LDT Bin
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

-- LDT TYPES (only lstack is defined here)
local LDT_TYPE_LSTACK = "LSTACK";

-- ++====================++
-- || INTERNAL BIN NAMES || -- Local, but global to this module
-- ++====================++
-- The Top Rec LDT bin is named by the user -- so there's no hardcoded name
-- for each used LDT bin.
--
-- In the main record, there is one special hardcoded bin -- that holds
-- some shared information for all LDTs.
-- Note the 14 character limit on Aerospike Bin Names.
--                         123456789ABCDE
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
local SUBREC_PROP_BIN   = "SR_PROP_BIN";
--
-- The Lso Data Records (LDRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_LIST_BIN      = "LdrListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- The Cold Dir Records use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
local COLD_DIR_LIST_BIN = "ColdDirListBin"; 
local COLD_DIR_CTRL_BIN = "ColdDirCtrlBin";

-- The Existence Sub-Records (ESRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above (and that might be all)

-- ++===============++
-- || Package Names ||
-- ++===============++
-- Package Names for "pre-packaged" settings:
local PackageStandardList        = "StandardList";
local PackageTestModeList        = "TestModeList";
local PackageTestModeBinary      = "TestModeBinary";
-- Specific production use: 
-- (*) A List Value (a 5 part tuple)
-- (*) Special, packed (compressed) Binary storage
local PackageProdListValBinStore = "ProdListValBinStore";
local PackageDebugModeList       = "DebugModeList";
local PackageDebugModeBinary     = "DebugModeBinary";

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are four main Record Types used in the LSO Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSO bin
-- (*) EsrRec: The Existence SubRecord (ESR) that coordinates all child
--             subrecs for a given LDT.
-- (*) LdrRec: the LSO Data Record (LDR) that holds user Data.
-- (*) ColdDirRec: The Record that holds a list of Sub Record Digests
--     (i.e. record pointers) to the LDR Data Records.  The Cold list is
--     a linked list of Directory pages;  each dir contains a list of
--     digests (record pointers) to the LDR data pages.
-- <+> Naming Conventions:
--   + All Field names (e.g. lsoMap[StoreMode]) begin with Upper Case
--   + All variable names (e.g. lsoMap[StoreMode]) begin with lower Case
--   + As discussed below, all Map KeyField names are INDIRECTLY referenced
--     via descriptive variables that map to a single character (to save
--     space when the entire map is msg-packed into a record bin).
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec[LDR_CTRL_BIN]);
--
-- <+> Recent Change in LdtMap Use: (6/21/2013 tjl)
--   + In order to maintain a common access mechanism to all LDTs, AND to
--     limit the amount of data that must be "un-msg-packed" when accessed,
--     we will use a common property map and a type-specific property map.
--     That means that the "lsoMap" that was the primary value in the LsoBin
--     is now a list, where lsoList[1] will always be the propMap and
--     lsoList[2] will always be the lsoMap.  In the server code, using "C",
--     we will sometimes read the lsoList[1] (the property map) in order to
--     perform some LDT management operations.
--   + Since Lua wraps up the LDT Control map as a self-contained object,
--     we are paying for storage in EACH LDT Bin for the map field names. 
--     Thus, even though we like long map field names for readability:
--     e.g.  lsoMap.HotEntryListItemCount, we don't want to spend the
--     space to store the large names in each and every LDT control map.
--     So -- we do another Lua Trick.  Rather than name the key of the
--     map value with a large name, we instead use a single character to
--     be the key value, but define a descriptive variable name to that
--     single character.  So, instead of using this in the code:
--     lsoMap.HotEntryListItemCount = 50;
--            123456789012345678901
--     (which would require 21 bytes of storage); We instead do this:
--     local HotEntryListItemCount='H';
--     lsoMap[HotEntryListItemCount] = 50;
--     Now, we're paying the storage cost for 'H' (1 byte) and the value.
--
--     So -- we have converted all of our LDT lua code to follow this
--     convention (fields become variables the reference a single char)
--     and the mapping of long name to single char will be done in the code.
-- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values. (There's no secret message hidden in these values).
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
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_EsrDigest             = 'E'; -- (All): Digest of ESR
local PM_RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
local PM_LogInfo               = 'L'; -- (All): Log Info (currently unused)
local PM_ParentDigest          = 'P'; -- (Subrec): Digest of TopRec
local PM_SelfDigest            = 'D'; -- (Subrec): Digest of THIS Record
-- Note: The TopRec keeps this in the single LDT Bin (RPM).
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Lso Data Record (LDR) Control Map Fields (Recall that each Map ALSO has
-- the PM (general property map) fields.
-- local LDR_StoreMode            = 'M'; !! Use Top LSO Entry
-- local LDR_ListEntryMax         = 'L'; !! Use top LSO entry
-- local LDR_ByteEntrySize        = 'e'; !! Use Top LSO Entry
local LDR_ByteEntryCount       = 'C'; -- Current Count of bytes used
-- local LDR_ByteCountMax         = 'X'; !! Use Top LSO Entry
-- local LDR_LogInfo              = 'I'; !! Not currently used
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Cold Directory Control Map::In addition to the General Property Map
local CDM_NextDirRec           = 'N';-- Ptr to next Cold Dir Page
local CDM_DigestCount          = 'C';-- Current Digest Count
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local M_StoreMode              = 'M';
local M_Transform              = 't';
local M_UnTransform            = 'u';
local M_LdrEntryCountMax       = 'e';
local M_LdrByteEntrySize       = 's';
local M_LdrByteCountMax        = 'b';
local M_HotEntryList           = 'H';
local M_HotEntryListItemCount  = 'L';
local M_HotListMax             = 'h';
local M_HotListTransfer        = 'X';
local M_WarmDigestList         = 'W';
local M_WarmTopFull            = 'F';
local M_WarmListDigestCount    = 'l';
local M_WarmListMax            = 'w';
local M_WarmListTransfer       = 'x';
local M_WarmTopChunkEntryCount = 'A';
local M_WarmTopChunkByteCount  = 'a';
local M_ColdDirListHead        = 'C';
local M_ColdTopFull            = 'f';
local M_ColdDataRecCount       = 'R';
local M_ColdDirRecCount        = 'r';
local M_ColdListMax            = 'c';
-- ------------------------------------------------------------------------
-- Maintain the LSO letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
--
-- A:M_WarmTopChunkEntryCount a:M_WarmTopChunkByteCount 0:
-- B:                         b:M_LdrByteCountMax       1:
-- C:M_ColdDirListHead        c:M_ColdListMax           2:
-- D:                         d:                        3:
-- E:                         e:M_LdrEntryCountMax      4:
-- F:M_WarmTopFull            f:M_ColdTopFull           5:
-- G:                         g:                        6:
-- H:M_HotEntryList           h:M_HotListMax            7:
-- I:                         i:                        8:
-- J:                         j:                        9:
-- K:                         k:                  
-- L:M_HotEntryListItemCount  l:M_WarmListDigestCount
-- M:M_StoreMode              m:
-- N:                         n:
-- O:                         o:
-- P:                         p:
-- Q:                         q:
-- R:M_ColdDataRecCount       r:M_ColdDirRecCount
-- S:                         s:M_LdrByteEntrySize
-- T:                         t:M_Transform
-- U:                         u:M_UnTransform
-- V:                         v:
-- W:M_WarmDigestList         w:M_WarmListMax
-- X:M_HotListTransfer        x:M_WarmListTransfer
-- Y:                         y:
-- Z:                         z:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  

-- ======================================================================
-- local function lsoSummary( lsoList ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsoMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function lsoSummary( lsoList )
  if ( lsoList == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    return "EMPTY LDT BIN VALUE";
  end

  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];
  if( propMap[PM_Magic] ~= MAGIC ) then
    return "BROKEN MAP--No Magic";
  end;

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();

  -- Properties
  resultMap.SUMMARY              = "LSO Summary";
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  
  -- General LSO Parms:
  resultMap.StoreMode            = lsoMap[M_StoreMode];
  resultMap.Transform            = lsoMap[M_Transform];
  resultMap.UnTransform          = lsoMap[M_UnTransform];

  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = lsoMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = lsoMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = lsoMap[M_LdrByteCountMax];
  --
  -- Hot Entry List Settings: List of User Entries
  resultMap.HotListMax            = lsoMap[M_HotListMax];
  resultMap.HotListTransfer       = lsoMap[M_HotListTransfer];
  resultMap.HotEntryListItemCount = lsoMap[M_HotEntryListItemCount];

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  resultMap.WarmListMax           = lsoMap[M_WarmListMax];
  resultMap.WarmListTransfer      = lsoMap[M_WarmListTransfer];
  resultMap.WarmListDigestCount   = lsoMap[M_WarmListDigestCount];

  -- Cold Directory List Settings: List of Directory Pages
  resultMap.ColdListMax           = lsoMap[M_ColdListMax];
  resultMap.ColdListDirRecCount   = lsoMap[M_ColdListDirRecCount];
  resultMap.ColdListDataRecCount  = lsoMap[M_ColdListDataRecCount];

  return resultMap;
end -- lsoSummary()

-- ======================================================================
-- Make it easier to use lsoSummary(): Have a String version.
-- ======================================================================
local function lsoSummaryString( lsoList )
    return tostring( lsoSummary( lsoList ) );
end

--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Notes on Configuration:
-- (*) In order to make the LSO code as efficient as possible, we want
--     to pick the best combination of configuration values for the Hot,
--     Warm and Cold Lists -- so that data transfers from one list to
--     the next with minimal storage upset and runtime management.
--     Similarly, we want the transfer from the LISTS to the Data pages
--     and Data Directories to be as efficient as possible.
-- (*) The HotEntryList should be the same size as the LDR Page that
--     holds the Data entries.
-- (*) The HotListTransfer should be half or one quarter the size of the
--     HotList -- so that even amounts can be transfered to the warm list.
-- (*) The WarmDigestList should be the same size as the DigestList that
--     is in the ColdDirectory Page
-- (*) The WarmListTransfer should be half or one quarter the size of the
--     list -- so that even amounts can be transfered to the cold list.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- initializeLso:
-- ======================================================================
-- Set up the LSO Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSO BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSO
-- behavior.  Thus this function represents the "type" LSO MAP -- all
-- LSO control fields are defined here.
-- The LsoMap is obtained using the user's LSO Bin Name:
-- lsoMap = 
-- local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
-- local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
-- local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
-- local RT_ESR = 4; -- 0x4: Existence Sub Record
-- ======================================================================
local function initializeLso( topRec, lsoBinName )
  local meth = "initializeLso()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific Lso Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local lsoMap = map();
  local lsoList = list();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount] = 0; -- A count of all items in the stack
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LSTACK; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = lsoBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]    = nil; -- not set yet.

  -- Specific LSO Parms: Held in LsoMap
  lsoMap[M_StoreMode]  = SM_LIST; -- SM_LIST or SM_BINARY:

  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax]= 100;  -- Max # of Data Chunk items (List Mode)
  lsoMap[M_LdrByteEntrySize]=  0;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax] =   0; -- Max # of Data Chunk Bytes (binary mode)

  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotEntryList]         = list(); -- the list of data entries
  lsoMap[M_HotEntryListItemCount]=   0; -- Number of elements in the Top List
  lsoMap[M_HotListMax]           = 100; -- Max Number for the List(then xfer)
  lsoMap[M_HotListTransfer]      =  50; -- How much to Transfer at a time.

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmDigestList]        = list(); -- the list of digests for LDRs
  lsoMap[M_WarmTopFull] = false; -- true when top chunk is full (for next write)
  lsoMap[M_WarmListDigestCount]   = 0; -- Number of Warm Data Record Chunks
  lsoMap[M_WarmListMax]           = 100; -- Number of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer]      = 2; -- Number of Warm Data Record Chunks
  lsoMap[M_WarmTopChunkEntryCount]= 0; -- Count of entries in top warm chunk
  lsoMap[M_WarmTopChunkByteCount] = 0; -- Count of bytes used in top warm Chunk

  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdDirListHead]= 0; -- Head (Rec Digest) of the Cold List Dir Chain
  lsoMap[M_ColdTopFull]    = false; -- true when cold head is full (next write)
  lsoMap[M_ColdDataRecCount]= 0; -- # of Cold DATA Records (data chunks)
  lsoMap[M_ColdDirRecCount]= 0; -- # of Cold DIRECTORY Records
  lsoMap[M_ColdListMax]    = 100; -- # of list entries in a Cold list dir node

  -- Put our new maps in a list, in the record, then store the record.
  list.append( lsoList, propMap );
  list.append( lsoList, lsoMap );
  topRec[lsoBinName]            = lsoList;

  GP=F and trace("[DEBUG]: <%s:%s> : Lso Summary after Init(%s)",
      MOD, meth , lsoSummaryString(lsoList));


  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return lsoList;
end -- initializeLso()

-- ======================================================================
-- initPropMap( propMap, subDigest, topDigest, rtFlag, lsoMap )
-- ======================================================================
-- Set up the LDR Property Map (one PM per LDT)
-- Parms:
-- (*) propMap: 
-- (*) subDigest:
-- (*) topDigest:
-- (*) rtFlag:
-- (*) lsoMap:
-- ======================================================================
local function initPropMap( propMap, subDigest, topDigest, rtFlag, lsoMap )
  local meth = "initPropMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );


  GP=F and trace("[HEY!!!]: <%s:%s> NOT YET FINISHED!!!  ", MOD, meth );

end -- initPropMap()

-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- ======================================================================
local function setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";
  GP=F and trace("[ENTER]<%s:%s>", MOD, meth );

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if( topRec[REC_LDT_CTRL_BIN] == nil ) then
    GP=F and trace("[DEBUG]<%s:%s>Creating Record LDT Map", MOD, meth );
    record.set_type( topRec, RT_LDT );
    recPropMap = map();
    recPropMap[RPM_Vinfo] = 99; -- to be replaced later - on the server side.
    recPropMap[RPM_LdtCount] = 1; -- this is the first one.
    recPropMap[RPM_Magic] = MAGIC;
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM_LdtCount];
    recPropMap[RPM_LdtCount] = ldtCount + 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    GP=F and trace("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1 );
  end

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
  return rc;
end -- setLdtRecordType()

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
local function createAndInitESR( topRec, lsoList )
  local meth = "createAndInitESR()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  local rc = 0;
  local esr       = aerospike:create_subrec( topRec );
  local esrDigest = record.digest( esr );
  local topDigest = record.digest( topRec );
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  print("THIS METHOD PROBABLY HAS TO CHANGE -- ESR COMES LATER\n");

  propMap[PM_RecType]   = RT_ESR;
  propMap[PM_ParentDigest] = topDigest; -- Parent
  propMap[PM_EsrDigest] = esrDigest; -- Self

  initPropMap( propMap, esrDigest, topDigest, RT_ESR, lsoList );

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  setLdtRecordType( topRec );

  GP=F and trace("[DEBUG]:<%s:%s>About to call record.set_flags(Bin(%s)F(%s))",
    MOD, meth, propMap[PM_BinName], tostring(BF_LDT_BIN) );

  record.set_flags( topRec, propMap[PM_BinName], BF_LDT_BIN );
  GP=F and trace("[DEBUG]: <%s:%s> Back from calling record.set_flags()",
    MOD, meth );

  -- Set the record type as "ESR"
  trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esr, RT_ESR );
  trace("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );

  -- Set the Property ControlMap for the ESR, and assign the parent Digest
  -- Note that we use our standard convention for property maps - all subrecs
  -- have a property map.
  local propMap = map();
  propMap[PM_ParentDigest] = topDigest;
  propMap[PM_EsrDigest] = esrDigest; -- Point to Self (mostly for tracing)

  esr[SUBREC_PROP_BIN] = propMap;

  GP=F and trace("[EXIT]: <%s:%s> Leaving with ESR Digest(%s)",
    MOD, meth, tostring(esrDigest));

  aerospike:update_subrec( esr );

  return esrDigest;

end -- createAndInitESR()

-- ======================================================================
-- initializeLdrMap()
-- ======================================================================
-- Set the values in a LSO Data Record (LDR) Control Bin map. LDR Records
-- hold the actual data for both the WarmList and ColdList.
-- This function represents the "type" LDR MAP -- all fields are
-- defined here.
-- Here are the in an LDR Record:
-- (*) ldrRec[LDR_PROP_BIN]: The propery Map (defined here)
-- (*) ldrRec[LDR_CTRL_BIN]: The control Map (defined here)
-- (*) ldrRec[LDR_LIST_BIN]: The Data Entry List (when in list mode)
-- (*) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
--
-- When we call this method, we have just created a LDT SubRecord.  Thus,
-- we must check to see if that is the FIRST one, and if so, we must also
-- create the Existence Sub-Record for this LDT.
-- ======================================================================
local function initializeLdrMap( topRec, ldrRec, ldrPropMap, ldrMap, lsoList)
  local meth = "initializeLdrMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  local lsoPropMap = lsoList[1];
  local lsoMap     = lsoList[2];
  local binName = lsoPropMap[PM_BinName];

  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  ldrPropMap[PM_SelfDigest]   = record.digest( ldrRec );
  --  Not doing Log stuff yet
  --  ldrPropMap[PM_LogInfo]      = lsoPropMap[M_LogInfo];

  --  Use Top level LSO entry for mode and max values
  ldrMap[LDR_ByteEntrySize]   = lsoMap[M_LdrByteEntrySize];
  ldrMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries

  if( lsoPropMap[PM_EsrDigest] == nil or lsoPropMap[PM_EsrDigest] == 0 ) then
    lsoPropMap[PM_EsrDigest] = createAndInitESR( topRec, lsoList );
  end

end -- initializeLdrMap()


-- ======================================================================
-- initializeColdDirMap()
-- ======================================================================
-- Set the default values in a Cold Directory Record. ColdDir records
-- contain a list of digests that reference LDRs (above).
-- This function represents the "type" ColdDir MAP -- all fields are
-- defined here.
-- There are two bins in a ColdDir Record:
-- (1) ldrRec[COLD_DIR_CTRL_BIN]: The control Map (defined here)
-- (2) ldrRec[COLD_DIR_LIST_BIN]: The Digest List
-- Parms:
-- (*) topRec
-- (*) coldDirRec
-- (*) coldDirPropMap
-- (*) coldDirMap
-- (*) lsoList
-- ======================================================================
local function initializeColdDirMap( topRec, cdRec, cdPropMap, cdMap, lsoList )
  local meth = "initializeColdDirMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  local lsoPropMap = lsoList[1];
  local lsoMap     = lsoList[2];
  
  cdPropMap[PM_ParentDigest] = record.digest( topRec );
  cdPropMap[PM_SelfDigest] = record.digest( cdRec );

  cdMap[CDM_NextDirRec] = 0; -- no other Dir Records (yet).
  cdMap[CDM_DigestCount] = 0; -- no digests in the list -- yet.

end -- initializeColdDirMap()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- These are all local functions to this module and serve various
-- utility and assistance functions.
-- ======================================================================
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
local function packageStandardList( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_LIST;
  lsoMap[M_Transform]        = nil;
  lsoMap[M_UnTransform]      = nil;
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_LIST;
  lsoMap[M_Transform]        = nil;
  lsoMap[M_UnTransform]      = nil;
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_BINARY;
  lsoMap[M_Transform]        = "compressTest4";
  lsoMap[M_UnTransform]      = "unCompressTest4";
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
end -- packageTestModeBinary()

-- ======================================================================
-- Package = "ProdListValBinStore";
-- Specific Production Use: 
-- (*) Tuple value (5 fields of integers)
-- (*) Transforms
-- (*) Binary Storage (uses a compacted representation)
-- ======================================================================
local function packageProdListValBinStore( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_BINARY;
  lsoMap[M_Transform]        = "listCompress_5_18";
  lsoMap[M_UnTransform]      = "listUnCompress_5_18";
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 200; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 18;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 100; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 100; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 100; -- # of list entries in a Cold dir node
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
local function packageDebugModeList( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_LIST;
  lsoMap[M_Transform]        = nil;
  lsoMap[M_UnTransform]      = nil;
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 4; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use BINARY MODE.
-- ======================================================================
local function packageDebugModeBinary( lsoMap )
  -- General LSO Parms:
  lsoMap[M_StoreMode]        = SM_BINARY;
  lsoMap[M_Transform]        = "compressTest4";
  lsoMap[M_UnTransform]      = "unCompressTest4";
  -- LSO Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  lsoMap[M_LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  lsoMap[M_LdrByteEntrySize] = 16;  -- Byte size of a fixed size Byte Entry
  lsoMap[M_LdrByteCountMax]  = 65; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap[M_HotListMax]       = 4; -- Max # for the List, when we transfer
  lsoMap[M_HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap[M_WarmListMax]      = 4; -- # of Warm Data Record Chunks
  lsoMap[M_WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap[M_ColdListMax]      = 4; -- # of list entries in a Cold dir node
end -- packageDebugModeBinary()

-- ======================================================================
-- adjustLsoList:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LsoMap:
-- Parms:
-- (*) lsoList: the main LSO Bin value (propMap, lsoMap)
-- (*) argListMap: Map of LSO Settings 
-- Return: The updated LsoList
-- ======================================================================
local function adjustLsoList( lsoList, argListMap )
  local meth = "adjustLsoList()";
  local propMap = lsoList[1];
  local lsoMap = lsoList[2];

  GP=F and trace("[ENTER]: <%s:%s>:: LsoList(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(lsoList), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the stackCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));


  for name, value in map.pairs( argListMap ) do
      GP=F and trace("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s)",
          MOD, meth, tostring( name ), tostring( value ));

      -- Process our "prepackaged" settings first:
      -- (*) StandardList: Generic starting mode.
      -- (*) TestMode List Mode: Small sizes to exercise the structure.
      -- (*) TestMode Binary Mode: Employ UDF transform and use small sizes
      --     to exercise the structure.
      -- (*) Production Customer Settings:
      --     - Binary Mode, Compress UDF, High Performance Settings.
      -- NOTE: Eventually, these "packages" will be installed in either
      -- a separate "package" lua file, or possibly in the UdfFunctionTable.
      -- Regardless though -- they will move out of this main file, except
      -- maybe for the "standard" packages.
      if name == "Package" and type( value ) == "string" then
        -- Figure out WHICH package we're going to deploy:
        if value == PackageStandardList then
            packageStandardList( lsoMap );
        elseif value == PackageTestModeList then
            packageTestModeList( lsoMap );
        elseif value == PackageTestModeBinary then
            packageTestModeBinary( lsoMap );
        elseif value == PackageProdListValBinStore then
            packageProdListValBinStore( lsoMap );
        elseif value == PackageDebugModeList then
            packageDebugModeList( lsoMap );
        elseif value == PackageDebugModeBinary then
            packageDebugModeBinary( lsoMap );
        end
      elseif name == "StoreMode" and type( value )  == "string" then
        -- Verify it's a valid value
        if value == SM_LIST or value == SM_BINARY then
          lsoMap[M_StoreMode] = value;
        end
      elseif name == "HotListSize"  and type( value )  == "number" then
        if value >= 10 and value <= 500 then
          lsoMap[M_HotListMax] = value;
        end
      elseif name == "HotListTransfer" and type( value ) == "number" then
        if value >= 2 and value <= ( lsoMap[M_HotListMax] - 2 ) then
          argListMap.HotListTransfer = value;
        end
      elseif name == "ByteEntrySize" and type( value ) == "number" then
        if value > 0 and value <= 4000 then
          lsoMap[M_LdrByteEntrySize] = value;
        end
      end
  end -- for each argument
      
  -- Do we need to reassign map to list?
  lsoList[2] = lsoMap;

  GP=F and trace("[EXIT]:<%s:%s>:LsoList after Init(%s)",
    MOD,meth,tostring(lsoList));
  return lsoList;
end -- adjustLsoList
-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
local function summarizeList( myList )
  if( myList == nil ) then return "NULL LIST"; end;

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
-- ldrChunkSummary( ldrChunk )
-- ======================================================================
-- Print out interesting stats about this LDR Chunk Record
-- ======================================================================
local function  ldrChunkSummary( ldrChunkRecord ) 
  if( ldrChunkRecord  == nil ) then
    return "NULL Data Chunk (LDR) RECORD";
  end;
  if( ldrChunkRecord[LDR_CTRL_BIN]  == nil ) then
    return "NULL LDR CTRL BIN";
  end;
  if( ldrChunkRecord[SUBREC_PROP_BIN]  == nil ) then
    return "NULL LDR PROPERTY BIN";
  end;

  local resultMap = map();
  local ldrMap = ldrChunkRecord[LDR_CTRL_BIN];
  local ldrPropMap = ldrChunkRecord[SUBREC_PROP_BIN];

  resultMap.SelfDigest   = ldrPropMap[PM_SelfDigest];
  resultMap.ParentDigest   = ldrPropMap[PM_ParentDigest];

  resultMap.WarmList = ldrChunkRecord[LDR_LIST_BIN];
  resultMap.ListSize = list.size( resultMap.WarmList );

  return tostring( resultMap );
end -- ldrChunkSummary()

-- ======================================================================
-- coldDirRecSummary( coldDirRec )
-- ======================================================================
-- Print out interesting stats about this Cold Directory Rec
-- ======================================================================
local function  coldDirRecSummary( coldDirRec )
  if( coldDirRec  == nil ) then return "NULL COLD DIR RECORD"; end;
  if( coldDirRec[COLD_DIR_CTRL_BIN] == nil ) then
    return "NULL COLD DIR RECORD CONTROL MAP";
  end;

  local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

  return tostring( coldDirMap );
end -- coldDirRecSummary()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- General LIST Read/Write(entry list, digest list) and LDR FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The same mechanisms are used in different contexts.  The HotList
-- Entrylist -- is similar to the EntryList in the Warm List.  The 
-- DigestList in the WarmList is similar to the ColdDir digest list in
-- the Cold List.  LDRs pointed to in the Warmlist are the same as the
-- LDRs pointed to in the cold list.

-- ======================================================================
-- readEntryList()
-- ======================================================================
-- This method reads the entry list from Hot, Warm and Cold Lists.
-- It examines each entry, applies the inner UDF function (if applicable)
-- and appends viable candidates to the result list.
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) lsoList:
--   (*) entryList:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readEntryList( resultList, lsoList, entryList, count,
    func, fargs, all)

  local meth = "readEntryList()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
      MOD,meth,tostring(count), tostring(func), tostring(fargs),tostring(all));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  local doUnTransform = false; 
  if( lsoMap[M_UnTransform] ~= nil ) then
    doUnTransform = true; 
  end

  local applyFilter = false;
  if func ~= nil and fargs ~= nil then
    applyFilter = true;
  end

  -- Iterate thru the entryList, gathering up items in the result list.
  -- There are two modes:
  -- (*) ALL Mode: Read the entire list, return all that qualify
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local numRead = 0;
  local numToRead = 0;
  local listSize = list.size( entryList );
  if all == true or count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  local readValue;
  for i = listSize, 1, -1 do

    -- Apply the transform to the item, if present
    if doUnTransform == true then -- apply the transform
      readValue = functionTable[lsoMap[M_UnTransform]]( entryList[i] );
    else
      readValue = entryList[i];
    end

    -- After the transform, we can apply the filter, if it is present.  If
    -- the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if applyFilter == true then
      resultValue = functionTable[func]( readValue, fargs );
    else
      resultValue = readValue;
    end

    list.append( resultList, readValue );
--    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
--      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == false then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s)",
        MOD, meth, numRead, summarizeList( resultList ));
      return numRead;
    end
  end -- for each entry in the list

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readEntryList()

-- ======================================================================
-- readByteArray()
-- ======================================================================
-- This method reads the entry list from Warm and Cold List Pages.
-- In each LSO Data Record (LDR), there are three Bins:  A Control Bin,
-- a List Bin (a List() of entries), and a Binary Bin (Compacted Bytes).
-- Similar to its sibling method (readEntryList), readByteArray() pulls a Byte
-- entry from the compact Byte array, applies the (assumed) UDF, and then
-- passes the resulting value back to the caller via the resultList.
--
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) lsoList
--   (*) LDR Chunk Page:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readByteArray( resultList, lsoList, ldrChunk, count,
                              func, fargs, all)
  local meth = "readByteArray()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
    MOD,meth,tostring(count), tostring(func), tostring(fargs), tostring(all));
            
  local lsoMap = lsoList[2];

  local doUnTransform = false;
  if( lsoMap[M_UnTransform] ~= nil ) then
    doUnTransform = true;
  end

  local applyFilter = false;
  if func ~= nil and fargs ~= nil then
    applyFilter = true;
  end

  -- Note: functionTable is "global" to this module, defined at top of file.

  -- Iterate thru the BYTE structure, gathering up items in the result list.
  -- There are two modes:
  -- (*) ALL Mode: Read the entire list, return all that qualify
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local ldrMap = ldrChunk[LDR_CTRL_BIN];
  local byteArray = ldrChunk[LDR_BNRY_BIN];
  local numRead = 0;
  local numToRead = 0;
  local listSize = ldrMap[LDR_ByteEntryCount]; -- Number of Entries
  local entrySize = lsoMap[M_LdrByteEntrySize]; -- Entry Size in Bytes
  -- When in binary mode, we rely on the LDR page control structure to track
  -- the ENTRY COUNT and the ENTRY SIZE.  Just like walking a list, we
  -- move thru the BYTE value by "EntrySize" amounts.  We will try as much
  -- as possible to treat this as a list, even though we access it directly
  -- as an array.
  --
  if all == true or count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  -- The BINARY information is held in the page's control info
  -- Current Item Count
  -- Current Size (items must be a fixed size)
  -- Max bytes allowed in the ByteBlock.
  -- Example: EntrySize = 10
  -- Address of Entry 1: 0
  -- Address of Entry 2: 10
  -- Address of Entry N: (N - 1) * EntrySize
  -- WARNING!!!  Unlike C Buffers, which start at ZERO, this byte type
  -- starts at ONE!!!!!!
  --
  -- 12345678901234567890 ...  01234567890
  -- +---------+---------+------+---------+
  -- | Entry 1 | Entry 2 | .... | Entry N | 
  -- +---------+---------+------+---------+
  --                            A
  -- To Read:  Start Here ------+  (at the beginning of the LAST entry)
  --           and move BACK towards the front.
  local readValue;
  local byteValue;
  local byteIndex = 0; -- our direct position in the byte array.
  GP=F and trace("[DEBUG]:<%s:%s>Starting loop Byte Array(%s) ListSize(%d)",
      MOD, meth, tostring(byteArray), listSize );
  for i = (listSize - 1), 0, -1 do

    byteIndex = 1 + (i * entrySize);
    byteValue = bytes.get_bytes( byteArray, byteIndex, entrySize );

--  GP=F and trace("[DEBUG]:<%s:%s>: In Loop: i(%d) BI(%d) BV(%s)",
--    MOD, meth, i, byteIndex, tostring( byteValue ));

    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doUnTransform == true then -- apply the "UnTransform" function
      readValue = functionTable[lsoMap[M_UnTransform]]( byteValue );
    else
      readValue = byteValue;
    end

    -- After the transform, we can apply the filter, if it is present.  If
    -- the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if applyFilter == true then
      resultValue = functionTable[func]( readValue, fargs );
    else
      resultValue = readValue;
    end

    -- If the value passes the filter (or if there is no filter), then add
    -- it to the result list.
    if( resultValue ~= nil ) then
      list.append( resultList, resultValue );
    end

    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == false then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultList(%s)",
        MOD, meth, numRead, tostring( resultList ));
      return numRead;
    end
  end -- for each entry in the list (packed byte array)

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readByteArray()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Data Record (LDR) "Chunk" FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- LDR routines act specifically on the LDR "Data Chunk" records.

-- ======================================================================
-- ldrInsertList( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertList(ldrChunkRec,lsoMap,listIndex,insertList )
  local meth = "ldrInsertList()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  GP=F and trace("[DEBUG]<%s:%s> LSO MAP(%s)", MOD, meth, tostring(lsoMap));

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  local ldrValueList = ldrChunkRec[LDR_LIST_BIN];
  local chunkIndexStart = list.size( ldrValueList ) + 1;
  local ldrByteArray = ldrChunkRec[LDR_BNRY_BIN]; -- might be nil

  GP=F and trace("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSlotsAvailable = (lsoMap[M_LdrEntryCountMax] - chunkIndexStart) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap[M_WarmTopFull] = true; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):WTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and trace("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, listIndex, chunkIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec[LDR_CTRL_BIN] = ldrMap;
  ldrChunkRec[LDR_LIST_BIN] = ldrValueList;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()


-- ======================================================================
-- ldrInsertBytes( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this chunk's Byte Array.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- This method is similar to its sibling "ldrInsertList()", but rather
-- than add to the entry list in the chunk's LDR_LIST_BIN, it adds to the
-- byte array in the chunk's LDR_BNRY_BIN.
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertBytes( ldrChunkRec, lsoMap, listIndex, insertList )
  local meth = "ldrInsertBytes()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = lsoMap[M_LdrByteEntrySize];
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", MOD, meth);
    -- Let the caller handle the error.
    return -1; -- General Badness
  end

  local entryCount = 0;
  if( ldrMap[LDR_ByteEntryCount] ~= nil and
      ldrMap[LDR_ByteEntryCount] ~= 0 )
  then
    entryCount = ldrMap[LDR_ByteEntryCount];
  end
  GP=F and trace("[DEBUG]:<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount );

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local maxEntries = math.floor(lsoMap[M_LdrByteCountMax] / entrySize );
  local itemSlotsAvailable = maxEntries - entryCount;
  GP=F and
    trace("[DEBUG]: <%s:%s>:MaxEntries(%d) SlotsAvail(%d) #Total ToWrite(%d)",
    MOD, meth, maxEntries, itemSlotsAvailable, totalItemsToWrite );

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[DEBUG]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap[M_WarmTopFull] = true; -- Remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):WTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- Compute the new space we need in Bytes and either extend existing or
  -- allocate it fresh.
  local totalSpaceNeeded = (entryCount + newItemsStored) * entrySize;
  if ldrChunkRec[LDR_BNRY_BIN] == nil then
    ldrChunkRec[LDR_BNRY_BIN] = bytes( totalSpaceNeeded );
    GP=F and trace("[DEBUG]:<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));
  else
    GP=F and
    trace("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));

    bytes.set_len(ldrChunkRec[LDR_BNRY_BIN], totalSpaceNeeded );

    GP=F and
    trace("[DEBUG]:<%s:%s>AFTER: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));
  end
  local chunkByteArray = ldrChunkRec[LDR_BNRY_BIN];

  -- We're packing bytes into a byte array. Put each one in at a time,
  -- incrementing by "entrySize" for each insert value.
  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  -- Compute where we should start inserting in the Byte Array.
  -- WARNING!!! Unlike a C Buffer, This BYTE BUFFER starts at address 1,
  -- not zero.
  local chunkByteStart = 1 + (entryCount * entrySize);

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d) ByteStart(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable, chunkByteStart );

  local byteIndex;
  local insertItem;
  for i = 0, (newItemsStored - 1), 1 do
    byteIndex = chunkByteStart + (i * entrySize);
    insertItem = insertList[i+listIndex];

    GP=F and
    trace("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
      MOD, meth, tostring( chunkByteArray), i, tostring( insertItem ),
      byteIndex );

    bytes.put_bytes( chunkByteArray, byteIndex, insertItem );

    GP=F and trace("[DEBUG]: <%s:%s> Post Append: ByteArray(%s)",
      MOD, meth, tostring(chunkByteArray));
  end -- for each remaining entry

  -- Update the ctrl map with the new count
  ldrMap[LDR_ByteEntryCount] = entryCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring( chunkByteArray ));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec[LDR_CTRL_BIN] = ldrMap;
  ldrChunkRec[LDR_BNRY_BIN] = chunkByteArray;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()

-- ======================================================================
-- ldrInsert( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in SM_LIST mode or
-- SM_BINARY mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsert(ldrChunkRec,lsoMap,listIndex,insertList )
  local meth = "ldrInsert()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s), ChunkSummary(%s)",
    MOD, meth, listIndex, tostring( insertList ),ldrChunkSummary(ldrChunkRec));

  if lsoMap[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,lsoMap,listIndex,insertList );
  else
    return ldrInsertBytes(ldrChunkRec,lsoMap,listIndex,insertList );
  end
end -- ldrInsert()

-- ======================================================================
-- ldrChunkRead( ldrChunk, resultList, lsoList, count, func, fargs, all );
-- ======================================================================
-- Read ALL, or up to 'count' items from this chunk, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the chunk in FIFO order.
-- Parms:
-- (*) ldrChunk: Record object for the warm or cold LSO Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoList: Main LSO Control info
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to filter read items
-- (*) fargs: Function Argument list for inner UDF
-- Return: the NUMBER of items read from this chunk.
-- ======================================================================
local function ldrChunkRead( ldrChunk, resultList, lsoList, count,
                             func, fargs, all )
  local meth = "ldrChunkRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) All(%s)",
      MOD, meth, count, tostring(all));

  -- Extract the property map and lso control map from the lso bin list.
  -- local propMap = lsoList[1];
  local lsoMap  = lsoList[2];
  local storeMode = lsoMap[M_StoreMode];

  -- If the page is SM_BINARY mode, then we're using the "Binary" Bin
  -- LDR_BNRY_BIN, otherwise we're using the "List" Bin LDR_LIST_BIN.
  local numRead = 0;
  if lsoMap[M_StoreMode] == SM_LIST then
    local chunkList = ldrChunk[LDR_LIST_BIN];
    numRead = readEntryList(resultList, lsoList, chunkList, count,
                            func, fargs, all);
  else
    numRead = readByteArray(resultList, lsoList, ldrChunk, count,
                            func, fargs, all);
  end

  GP=F and trace("[EXIT]: <%s:%s> NumberRead(%d) ResultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- ldrChunkRead()
-- ======================================================================

-- ======================================================================
-- digestListRead(topRec, resultList, lsoList, Count, func, fargs, all)
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoList: Main LSO Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == true, read all items, regardless of "count".
-- Return: Return the amount read from the Digest List.
-- ======================================================================
local function digestListRead(topRec, resultList, lsoList, digestList, count,
                           func, fargs, all)
  local meth = "digestListRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) all(%s)",
      MOD, meth, count, tostring(all) );

  GP=F and trace("[DEBUG]: <%s:%s> Count(%d) DigList(%s) ResList(%s)",
      MOD, meth, count, tostring( digestList), tostring( resultList ));

  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- Process the DigestList bottom to top, pulling in each digest in
  -- turn, opening the chunk and reading records (as necessary), until
  -- we've read "count" items.  If the 'all' flag is true, then read 
  -- everything.
  -- NOTE: This method works for both the Warm and Cold lists.

  -- If we're using the "all" flag, then count just doesn't work.  Try to
  -- ignore counts entirely when the ALL flag is set.
  if all == true or count < 0 then count = 0; end
  local remaining = count;
  local totalAmountRead = 0;
  local chunkItemsRead = 0;
  local dirCount = list.size( digestList );
  local ldrChunk;
  local stringDigest;
  local status = 0;

  GP=F and trace("[DEBUG]:<%s:%s>:DirCount(%d)  Reading DigestList(%s)",
    MOD, meth, dirCount, tostring( digestList) );

  -- Read each Data Chunk, adding to the resultList, until we either bypass
  -- the readCount, or we hit the end (either readCount is large, or the ALL
  -- flag is set).
  for dirIndex = dirCount, 1, -1 do
    -- Record Digest MUST be in string form
    stringDigest = tostring(digestList[ dirIndex ]);
    GP=F and trace("[DEBUG]: <%s:%s>: Opening Data Chunk:Index(%d)Digest(%s):",
    MOD, meth, dirIndex, stringDigest );
    ldrChunk = aerospike:open_subrec( topRec, stringDigest );
    
    -- resultList is passed by reference and we can just add to it.
    chunkItemsRead =
    ldrChunkRead( ldrChunk, resultList, lsoList, remaining, func, fargs, all );
    totalAmountRead = totalAmountRead + chunkItemsRead;

    GP=F and
    trace("[DEBUG]:<%s:%s>:after ChunkRead:NumRead(%d)DirIndex(%d)ResList(%s)", 
      MOD, meth, chunkItemsRead, dirIndex, tostring( resultList ));
    -- Early exit ONLY when ALL flag is not set.
    if( all == false and
      ( chunkItemsRead >= remaining or totalAmountRead >= count ) )
    then
      GP=F and trace("[Early EXIT]:<%s:%s>totalAmountRead(%d) ResultList(%s) ",
        MOD, meth, totalAmountRead, tostring(resultList));
      status = aerospike:close_subrec( ldrChunk );
      return totalAmountRead;
    end

    -- status = aerospike:close_subrec( topRec, ldrChunk );
    status = aerospike:close_subrec( ldrChunk );
    GP=F and trace("[DEBUG]: <%s:%s> as:close() status(%s) ",
    MOD, meth, tostring( status ) );

    -- Get ready for the next iteration.  Adjust our numbers for the
    -- next round
    remaining = remaining - chunkItemsRead;
  end -- for each Data Chunk Record

  GP=F and trace("[EXIT]: <%s:%s> totalAmountRead(%d) ResultListSummary(%s) ",
  MOD, meth, totalAmountRead, summarizeList(resultList));
  return totalAmountRead;
end -- digestListRead()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- HOT LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The Hot List is an USER DATA ENTRY list that is managed IN THE RECORD.
-- The top N (most recent) values are held in the record, and then they
-- are aged out into the Warm List (a list of data pages) as they are
-- replaced by newer (more recent) data entries.  Hot List functions
-- directly manage the user data - and always in LIST form (not in
-- compact binary form).

-- ======================================================================
-- hotListRead( resultList, lsoList, count, func, fargs );
-- ======================================================================
-- Parms:
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoList: Main Lso Control Structure
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: Boolean: when true, read ALL
-- Return 'count' items from the Hot List
local function hotListRead( resultList, lsoList, count, func, fargs, all)
  local meth = "hotListRead()";
  GP=F and trace("[ENTER]:<%s:%s>Count(%d) All(%s)",
      MOD, meth, count, tostring( all ) );

  local lsoMap = lsoList[2];
  local hotList = lsoMap[M_HotEntryList];

  local numRead =
    readEntryList(resultList, lsoList, hotList, count, func, fargs, all);

  GP=F and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    MOD, meth, summarizeList(resultList) );
  return resultList;
end -- hotListRead()
-- ======================================================================

-- ======================================================================
-- extractHotListTransferList( lsoMap )
-- ======================================================================
-- Extract the oldest N elements (as defined in lsoMap) and create a
-- list that we return.  Also, reset the HotList to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTES:
-- (1) We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB_RECORD operations have succeeded.
-- (2) We don't need to use lsoList as a parameter -- lsoMap is ok here.
-- ======================================================================
local function extractHotListTransferList( lsoMap )
  local meth = "extractHotListTransferList()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap[M_HotListTransfer];
  local oldHotEntryList = lsoMap[M_HotEntryList];
  local newHotEntryList = list();
  local resultList = list.take( oldHotEntryList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  for i = 1, list.size(oldHotEntryList) - transAmount, 1 do 
    list.append( newHotEntryList, oldHotEntryList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldHotList(%s) NewHotList(%s) ResultList(%s)",
    MOD, meth, tostring(oldHotEntryList), tostring(newHotEntryList),
    tostring(resultList));

  -- Point to the new Hot List and update the Hot Count.
  lsoMap[M_HotEntryList] = newHotEntryList;
  oldHotEntryList = nil;
  local helic = lsoMap[M_HotEntryListItemCount];
  lsoMap[M_HotEntryListItemCount] = helic - transAmount;

  GP=F and trace("[EXIT]: <%s:%s> ResultList(%s)",
    MOD, meth, summarizeList(resultList));
  return resultList;
end -- extractHotListTransferList()


-- ======================================================================
-- hotListHasRoom( lsoMap, insertValue )
-- ======================================================================
-- Return true if there's room, otherwise return false.
-- (*) lsoMap: the map for the LSO Bin
-- (*) insertValue: the new value to be pushed on the stack
-- NOTE: This is in its own function because it is possible that we will
-- want to add more sophistication in the future.
local function hotListHasRoom( lsoMap, insertValue )
  local meth = "hotListHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> : ", MOD, meth );
  local result = true;  -- This is the usual case

  local hotListLimit = lsoMap[M_HotListMax];
  local hotList = lsoMap[M_HotEntryList];
  if list.size( hotList ) >= hotListLimit then
    return false;
  end

  GP=F and trace("[EXIT]: <%s:%s> Result(%s) : ", MOD, meth, tostring(result));
  return result;
end -- hotListHasRoom()

-- ======================================================================
-- hotListInsert()
-- ======================================================================
-- Insert a value at the end of the Hot Entry List.  The caller has 
-- already verified that space exists, so we can blindly do the insert.
--
-- The MODE of storage depends on what we see in the valueMap.  If the
-- valueMap holds a BINARY type, then we are going to store it in a special
-- binary bin.  Here are the cases:
-- (1) Warm List: The Chunk Record employs a List Bin and Binary Bin, where
--    the individual entries are packed.  In the Chunk Record, there is a
--    Map (control information) showing the status of the packed Binary bin.
-- (2) Cold List: Same Chunk format as the Warm List Chunk Record.
--
-- Change in plan -- All items go on the HotList, regardless of type.
-- Only when we transfer to Warm/Cold do we employ the COMPACT STORAGE
-- trick of packing bytes contiguously in the Binary Bin.
--
-- The Top LSO page (and the individual LDR chunk pages) have the control
-- data about the byte entries (entry size, entry count).
-- Parms:
-- (*) lsoList: the control structure for the LSO Bin
-- (*) newStorageValue: the new value to be pushed on the stack
local function hotListInsert( lsoList, newStorageValue  )
  local meth = "hotListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    MOD, meth, tostring(newStorageValue) );

  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- Update the hot list with a new element (and update the map)
  local hotList = lsoMap[M_HotEntryList];
  info("[HEY!!]<%s:%s> Appending to Hot List(%s)",MOD, meth,tostring(hotList));
  -- list.append( lsoMap[M_HotEntryList], newStorageValue );
  list.append( hotList, newStorageValue );
  lsoMap[M_HotEntryList] = hotList;
  --
  -- Update the count (overall count and hot list count)
  local itemCount = propMap[PM_ItemCount];
  propMap[PM_ItemCount] = (itemCount + 1);

  local hotCount = lsoMap[M_HotEntryListItemCount];
  lsoMap[M_HotEntryListItemCount] = (hotCount + 1);

  GP=F and trace("[EXIT]: <%s:%s> : LSO List Result(%s)",
    MOD, meth, tostring( lsoList ) );

  return 0;  -- all is well
end -- hotListInsert()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||         WARM LIST FUNCTIONS         ||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- ======================================================================
-- warmListChunkCreate( topRec, lsoList )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
local function   warmListChunkCreate( topRec, lsoList )
  local meth = "warmListChunkCreate()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newLdrChunkRecord = aerospike:create_subrec( topRec );
  local ldrPropMap = map();
  local ldrMap = map();
  local newChunkDigest = record.digest( newLdrChunkRecord );
  local lsoPropMap = lsoList[1];
  local lsoMap     = lsoList[2];
  local binName    = lsoPropMap[PM_BinName];

  initializeLdrMap( topRec, newLdrChunkRecord, ldrPropMap, ldrMap, lsoList );

  -- Assign Prop, Control info and List info to the LDR bins
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;
  newLdrChunkRecord[LDR_CTRL_BIN] = ldrMap;
  newLdrChunkRecord[LDR_LIST_BIN] = list();

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrMap) );

  aerospike:update_subrec( newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDigestList
  GP=F and trace("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(lsoMap[M_WarmDigestList]));
  list.append( lsoMap[M_WarmDigestList], newChunkDigest );
  GP=F and trace("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LsoMap(%s)CH(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(lsoMap),
    tostring( lsoMap[M_ColdDirListHead] ));
   
  -- Increment the Warm Count
  local warmChunkCount = lsoMap[M_WarmListDigestCount];
  lsoMap[M_WarmListDigestCount] = (warmChunkCount + 1);

  -- NOTE: This may not be needed -- we may wish to update the topRec ONLY
  -- after all of the underlying SUB-REC  operations have been done.
  -- Update the top (LSO) record with the newly updated lsoMap;
  topRec[ binName ] = lsoMap;

  GP=F and trace("[EXIT]: <%s:%s> Return(%s) ",
    MOD, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  warmListChunkCreate()

-- ======================================================================
-- extractWarmListTransferList( lsoList );
-- ======================================================================
-- Extract the oldest N digests from the WarmList (as defined in lsoMap)
-- and create a list that we return.  Also, reset the WarmList to exclude
-- these elements.  -- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTE: We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB-REC  operations have succeeded.
-- ======================================================================
local function extractWarmListTransferList( lsoList )
  local meth = "extractWarmListTransferList()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Extract the main property map and lso control map from the lsoList
  local lsoPropMap = lsoList[1];
  local lsoMap     = lsoList[2];

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap[M_WarmListTransfer];
  local oldWarmDigestList = lsoMap[M_WarmDigestList];
  local newWarmDigestList = list();
  local resultList = list.take( oldWarmDigestList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  for i = 1, list.size(oldWarmDigestList) - transAmount, 1 do 
    list.append( newWarmDigestList, oldWarmDigestList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldWarmList(%s) NewWarmList(%s)ResList(%s) ",
    MOD, meth, tostring(oldWarmDigestList), tostring(newWarmDigestList),
    tostring(resultList));

  -- Point to the new Warm List and update the Hot Count.
  lsoMap[M_WarmDigestList] = newWarmDigestList;
  oldWarmDigestList = nil;
  lsoMap[M_WarmListDigestCount] = lsoMap[M_WarmListDigestCount] - transAmount;

  GP=F and trace("[EXIT]: <%s:%s> ResultList(%s) LsoMap(%s)",
      MOD, meth, summarizeList(resultList), tostring(lsoMap));

  return resultList;
end -- extractWarmListTransferList()

  
-- ======================================================================
-- warmListHasRoom( lsoMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmListHasRoom( lsoMap )
  local meth = "warmListHasRoom()";
  local decision = 1; -- Start Optimistic (most times answer will be YES)
  GP=F and trace("[ENTER]: <%s:%s> Bin Map(%s)", 
    MOD, meth, tostring( lsoMap ));

  if lsoMap[M_WarmListDigestCount] >= lsoMap[M_WarmListMax] then
    decision = 0;
  end

  GP=F and trace("[EXIT]: <%s:%s> Decision(%d)", MOD, meth, decision );
  return decision;
end -- warmListHasRoom()


-- ======================================================================
-- warmListRead(topRec, resultList, lsoList, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: Pass the Warm list on to "digestListRead()" and let it do
-- all of the work.
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoList: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Warm Dir List.
-- ======================================================================
local function warmListRead(topRec, resultList, lsoList, count, func,
    fargs, all)

  local lsoMap  = lsoList[2];
  local digestList = lsoMap[M_WarmDigestList];

  return digestListRead(topRec, resultList, lsoList,
                          digestList, count, func, fargs, all);
end


-- ======================================================================
-- warmListGetTop( topRec, lsoMap )
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- ======================================================================
local function warmListGetTop( topRec, lsoMap )
  local meth = "warmListGetTop()";
  GP=F and trace("[ENTER]: <%s:%s> lsoMap(%s)", MOD, meth, tostring( lsoMap ));

  local warmDigestList = lsoMap[M_WarmDigestList];
  local stringDigest = tostring( warmDigestList[ list.size(warmDigestList) ]);

  GP=F and trace("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)", 
      MOD, meth, stringDigest, list.size( warmDigestList ));

  local topWarmChunk = aerospike:open_subrec( topRec, stringDigest );

  GP=F and trace("[EXIT]: <%s:%s> result(%s) ",
    MOD, meth, ldrChunkSummary( topWarmChunk ) );
  return topWarmChunk;
end -- warmListGetTop()
-- ======================================================================


-- ======================================================================
-- warmListInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the warm
-- dir list -- a directory of warm Lso Data Records that will contain 
-- the data entries.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lsoList: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function warmListInsert( topRec, lsoList, entryList )
  local meth = "warmListInsert()";
  local rc = 0;

  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[ENTER]: <%s:%s> WDL(%s)",
    MOD, meth, tostring(lsoMap[M_WarmDigestList]));

  GP=F and trace("[DEBUG]:<%s:%s> LSO LIST(%s)", MOD, meth, tostring(lsoList));

  local warmDigestList = lsoMap[M_WarmDigestList];
  local topWarmChunk;
  -- Whether we create a new one or open an existing one, we save the current
  -- count and close the record.
  -- Note that the last write may have filled up the warmTopChunk, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
  if list.size( warmDigestList ) == 0 or lsoMap[M_WarmTopFull] == true then
    GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
    topWarmChunk = warmListChunkCreate( topRec, lsoList ); -- create new
    lsoMap[M_WarmTopFull] = false; -- reset for next time.
  else
    GP=F and trace("[DEBUG]: <%s:%s> Calling Get TOP ", MOD, meth );
    topWarmChunk = warmListGetTop( topRec, lsoMap ); -- open existing
  end
  GP=F and trace("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) ", 
    MOD, meth, tostring( lsoMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalEntryCount = list.size( entryList );
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  local countWritten = ldrInsert( topWarmChunk, lsoMap, 1, entryList );
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
    error('Internal Error on insert(1)');
  end
  local itemsLeft = totalEntryCount - countWritten;
  if itemsLeft > 0 then
    aerospike:update_subrec( topWarmChunk );

    -- aerospike:close_subrec( topRec, topWarmChunk );
    aerospike:close_subrec( topWarmChunk );

    GP=F and trace("[DEBUG]:<%s:%s>Calling Chunk Create: AGAIN!!", MOD, meth );
    topWarmChunk = warmListChunkCreate( topRec, lsoMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot list.
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)",
    MOD, meth, tostring( entryList ), countWritten + 1);
    countWritten =
        ldrInsert( topWarmChunk, lsoMap, countWritten+1, entryList );
    if( countWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
      error('Internal Error on insert(2)');
    end
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) ",
        MOD, meth, countWritten, itemsLeft );
      error('Internal Error on insert(3)');
    end
  end

  -- NOTE: We do NOT have to update the WarmDigest Count here; that is done
  -- in the warmListChunkCreate() call.

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( lsoMap ));
  topRec[binName] = lsoMap;

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    MOD, meth, ldrChunkSummary( topWarmChunk ));

  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC  Update ", MOD, meth );
  local status = aerospike:update_subrec( topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",MOD,meth, tostring(status));
  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC  Close ", MOD, meth );

  -- status = aerospike:close_subrec( topRec, topWarmChunk );
  status = aerospike:close_subrec( topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));

  -- Notice that the TOTAL ITEM COUNT of the LSO doesn't change.  We've only
  -- moved entries from the hot list to the warm list.

  return rc;
end -- warmListInsert


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- COLD LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- ======================================================================
-- coldDirHeadCreate()
-- ======================================================================
-- Set up a new Head Directory page for the cold list.  The Cold List Dir
-- pages each hold a list of digests to data pages.  Note that
-- the data pages (LDR pages) are already built from the warm list, so
-- the cold list just holds those LDR digests after the record agest out
-- of the warm list. 
-- Parms:
-- (*) topRec: the top record -- needed when we create a new dir and LDR
-- (*) lsoList: the control map of the top record
local function coldDirHeadCreate( topRec, lsoList )
  local meth = "coldDirHeadCreate()";
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[ENTER]: <%s:%s>: lsoMap(%s)", MOD, meth, tostring(lsoMap));

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newColdHead    = aerospike:create_subrec( topRec );
  local coldDirMap     = map();
  local coldDirPropMap = map();
  initializeColdDirMap(topRec,newColdHead,coldDirPropMap,coldDirMap,lsoList);

  -- Update our global counts ==> One more Cold Dir Record.
  local coldDirRecCount = lsoMap[M_ColdDirRecCount];
  lsoMap[M_ColdDirRecCount] = coldDirRecCount + 1;

  -- Plug this directory into the chain of Dir Records (starting at HEAD).
  coldDirMap[CDM_NextDirRec] = lsoMap[M_ColdDirListHead];
  lsoMap[M_ColdDirListHead] = coldDirPropMap[PM_SelfDigest];--set in initDirMap

  GP=F and trace("[DEBUG]: <%s:%s> Just Set ColdHead = (%s) Cold Next = (%s)",
    MOD, meth, tostring(lsoMap[M_ColdDirListHead]),
    tostring(coldDirPropMap[CDM_NextDirRec]));

  GP=F and trace("[REVIEW]: <%s:%s> LSOMAP = (%s) COLD DIR PROP MAP = (%s)",
    MOD, meth, tostring(lsoMap), tostring(coldDirPropMap));

  -- Save our updates in the records
  newColdHead[COLD_DIR_LIST_BIN] = list(); -- allocate a new digest list
  newColdHead[COLD_DIR_CTRL_BIN] = coldDirMap;
  newColdHead[SUBREC_PROP_BIN] = coldDirPropMap;-- same binName for all subrecs

  aerospike:update_subrec( newColdHead );

  -- NOTE: We don't want to update the TOP RECORD until we know that
  -- the  underlying children record operations are complete.
  -- However, we can update topRec here, since that won't get written back
  -- to storage until there's an explicit update_subrec() call.
  topRec[ binName ] = lsoMap;

  GP=F and trace("[EXIT]: <%s:%s> New Cold Head Record(%s) ",
    MOD, meth, coldDirRecSummary( newColdHead ));
  return newColdHead;
end --  coldDirHeadCreate()()

-- ======================================================================
-- coldDirRecInsert(lsoList, coldHeadRec,digestListIndex,digestList)
-- ======================================================================
-- Insert as much as we can of "digestList", which is a list of digests
-- to LDRs, into a -- Cold Directory Page.  Return num written.
-- It is the caller's job to allocate a NEW Dir Rec page if not all of
-- digestList( digestListIndex to end) fits.
-- Parms:
-- (*) lsoList: the main control structure
-- (*) coldHeadRec: The Cold List Directory Record
-- (*) digestListIndex: The starting Read position in the list
-- (*) digestList: the list of digests to be inserted
-- Return: Number of digests written, -1 for error.
-- ======================================================================
local function coldDirRecInsert(lsoList,coldHeadRec,digestListIndex,digestList)
  local meth = "coldDirRecInsert()";
  local rc = 0;
  GP=F and trace("[ENTER]:<%s:%s> ColdHead(%s) ColdDigestList(%s)",
      MOD, meth, coldDirRecSummary(coldHeadRec), tostring( digestList ));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldDirList = coldHeadRec[COLD_DIR_LIST_BIN];
  local coldDirMax = lsoMap[M_ColdListMax];

  -- Write as much as we can into this Cold Dir Page.  If this is not the
  -- first time around the startIndex (digestListIndex) may be a value
  -- other than 1 (first position).
  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( digestList ) + 1 - digestListIndex;
  local itemSlotsAvailable = (coldDirMax - digestListIndex) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    MOD, meth, tostring( coldDirMap ));
    -- Deal with this at a higher level.
    return -1; -- nothing written, Error.  Bubble up to caller
  end

  -- If we EXACTLY fill up the ColdDirRec, then we flag that so the next Cold
  -- List Insert will know in advance to create a new ColdDirHEAD.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap[M_ColdTopFull] = true; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):CTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.  We don't expect
  -- to have a "binary mode" for just the digest list.  We could, but that
  -- would be extra complexity for very little gain.
  GP=F and trace("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, digestListIndex, list.size(digestList), newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( coldDirList, digestList[i + digestListIndex] );
  end -- for each remaining entry

  -- Update the Count of Digests on the page (should match list size).
  local digestCount = coldDirMap[CDM_DigestCount];
  coldDirMap[CDM_DigestCount] = digestCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post digest Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(coldDirMap), tostring(coldDirList));

  -- Store our modifications back into the Chunk Record Bins
  coldHeadRec[COLD_DIR_CTRL_BIN] = coldDirMap;
  coldHeadRec[COLD_DIR_LIST_BIN] = coldDirList;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) Digest List(%s) map(%s)",
    MOD, meth, newItemsStored, tostring( coldDirList), tostring(coldDirMap));

  return newItemsStored;
end -- coldDirRecInsert()


-- ======================================================================
-- coldListInsert()
-- ======================================================================
-- Insert "insertList", which is a list of digest entries, into the cold
-- dir page -- a directory of cold Lso Data Record digests that contain 
-- the actual data entries. Note that the data pages were built when the
-- warm list was created, so all we're doing now is moving the LDR page
-- DIGESTS -- not the data itself.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lsoList: the control map of the top record
-- (*) digestList: the list of digests to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function coldListInsert( topRec, lsoList, digestList )
  local meth = "coldListInsert()";
  local rc = 0;

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[ENTER]: <%s:%s> LSO Map Contents(%s) ",
      MOD, meth, tostring(lsoMap), tostring( digestList ));

  GP=F and trace("[DEBUG 0]:Map:WDL(%s)", tostring( lsoMap[M_WarmDigestList] ));

  local transferAmount = list.size( digestList );

  -- If we don't have a cold list, then we have to build one.  Also, if
  -- the current cold Head is completely full, then we also need to add
  -- a new one.
  local stringDigest;
  local coldHeadRec;

  local coldHeadDigest = lsoMap[M_ColdDirListHead];
  GP=F and trace("[DEBUG]<%s:%s>Cold List Head Digest(%s), ColdFullorNew(%s)",
      MOD, meth, tostring( coldHeadDigest), tostring(lsoMap[M_ColdTopFull]));

  if coldHeadDigest == nil or
     coldHeadDigest == 0 or
     lsoMap[M_ColdTopFull] == true
  then
    -- Create a new Cold Directory Head and link it in the Dir Chain.
    GP=F and trace("[DEBUG]:<%s:%s>:Creating FIRST NEW COLD HEAD", MOD, meth );
    coldHeadRec = coldDirHeadCreate( topRec, lsoList );
    coldHeadDigest = record.digest( coldHeadRec );
    stringDigest = tostring( coldHeadDigest );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Opening Existing COLD HEAD", MOD, meth );
    stringDigest = tostring( coldHeadDigest );
    coldHeadRec = aerospike:open_subrec( topRec, stringDigest );
  end

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldHeadList = coldHeadRec[COLD_DIR_LIST_BIN];

  GP=F and trace("[DEBUG]:<%s:%s>:Digest(%s) ColdHeadCtrl(%s) ColdHeadList(%s)",
    MOD, meth, tostring( stringDigest ), tostring( coldDirMap ),
    tostring( coldHeadList ));

  -- Iterate thru and transfer the "digestList" (which is a list of
  -- LDR data chunk record digests) into the coldDirHead.  If it doesn't all
  -- fit, then create a new coldDirHead and keep going.
  local digestsWritten = 0;
  local digestsLeft = transferAmount;
  local digestListIndex = 1; -- where in the insert list we copy from.
  while digestsLeft > 0 do
    digestsWritten =
      coldDirRecInsert(lsoList, coldHeadRec, digestListIndex, digestList);
    if( digestsWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Cold Dir Insert", MOD, meth);
      error('ERROR in Cold List Insert(1)');
    end
    digestsLeft = digestsLeft - digestsWritten;
    digestListIndex = digestListIndex + digestsWritten;
    -- If we have more to do -- then write/close the current coldHeadRec and
    -- allocate ANOTHER one (woo hoo).
    if digestsLeft > 0 then
      aerospike:update_subrec( coldHeadRec );
      aerospike:close_subrec( coldHeadRec );
      GP=F and trace("[DEBUG]: <%s:%s> Calling Cold DirHead Create: AGAIN!!",
          MOD, meth );
      coldHeadRec = coldDirHeadCreate( topRec, lsoList );
    end
  end -- while digests left to write.
  
  -- Update the Cold List Digest Count (add to cold, subtract from warm)
  local coldDataRecCount = lsoMap[M_ColdDataRecCount];
  lsoMap[M_ColdDataRecCount] = coldDataRecCount + transferAmount;

  local warmListCount = lsoMap[M_WarmListDigestCount];
  lsoMap[M_WarmListDigestCount] = warmListCount - transferAmount;

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( lsoMap ));
  topRec[ binName ] = lsoMap;

  GP=F and trace("[DEBUG]: <%s:%s> New Cold Head Save: Summary(%s) ",
    MOD, meth, coldDirRecSummary( coldHeadRec ));
  local status = aerospike:update_subrec( coldHeadRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",
    MOD,meth, tostring(status));

  status = aerospike:close_subrec( coldHeadRec );
  GP=F and trace("[EXIT]: <%s:%s> SUB-REC  Close Status(%s) RC(%d)",
    MOD,meth, tostring(status), rc );

  -- Note: warm->cold transfer only.  No new data added here.
  -- So, no new counts to upate (just warm/cold adjustments).

  return rc;
end -- coldListInsert


-- ======================================================================
-- coldListRead(topRec, resultList, lsoList, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: March down the Cold List Directory Pages (a linked list of
-- directory pages -- that each point to Lso Data Record "chunks") and
-- read "count" data entries.  Use the same ReadDigestList method as the
-- warm list.
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoList: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Cold Dir List.
-- ======================================================================
local function
coldListRead(topRec, resultList, lsoList, count, func, fargs, all)
  local meth = "coldListRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) All(%s) lsoMap(%s)",
      MOD, meth, count, tostring( all ), tostring( lsoMap ));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- If there is no Cold List, then return immediately -- nothing read.
  if(lsoMap[M_ColdDirListHead] == nil or lsoMap[M_ColdDirListHead] == 0) then
    GP=F and trace("[WARNING]: <%s:%s> LSO MAP COLD LIST Head is Nil/ZERO",
      MOD, meth, count, tostring( all ));
    return 0;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = lsoMap[M_ColdDirListHead];

  -- Outer loop -- Process each Cold Directory Page.  Each Cold Dir page
  -- holds a list of digests -- just like our WarmDigestList in the
  -- record, so the processing of that will be the same.
  -- Process the Linked List of Dir pages, head to tail
  local numRead = 0;
  local totalNumRead = 0;
  local countRemaining =  count;

  trace("[DEBUG]:<%s:%s>:Starting ColdDirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(coldDirRecDigest) );

  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    trace("[DEBUG]:<%s:%s>:Top of ColdDirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(coldDirRecDigest) );
    -- Open the Directory Page
    local stringDigest = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = aerospike:open_subrec( topRec, stringDigest );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

    GP=F and trace("[DEBUG]<%s:%s>Cold Dir subrec digest(%s) Map(%s) List(%s)",
      MOD, meth, stringDigest, tostring(coldDirMap),tostring(digestList));

    numRead = digestListRead(topRec, resultList, lsoList, digestList,
                            countRemaining, func, fargs, all)
    if numRead <= 0 then
        warn("[ERROR]:<%s:%s>:Cold List Read Error: Digest(%s)",
          MOD, meth, stringDigest );
          return numRead;
    end

    totalNumRead = totalNumRead + numRead;
    countRemaining = countRemaining - numRead;

    GP=F and trace("[DEBUG]:<%s:%s>:After Read: TotalRead(%d) NumRead(%d)",
          MOD, meth, totalNumRead, numRead );
    GP=F and trace("[DEBUG]:<%s:%s>:CountRemain(%d) NextDir(%s)",
          MOD, meth, countRemaining, tostring(coldDirMap[CDM_NextDirRec]));

    if countRemaining <= 0 or coldDirMap[CDM_NextDirRec] == 0 then
        GP=F and trace("[EARLY EXIT]:<%s:%s>:Cold Read: (%d) Items",
          MOD, meth, totalNumRead );
        aerospike:close_subrec( coldDirRec );
        return totalNumRead;
    end

    GP=F and trace("[DEBUG]:<%s:%s>Reading NEXT DIR:", MOD, meth );
    
    -- Ok, so now we've read ALL of the contents of a Directory Record
    -- and we're still not done.  Close the old dir, open the next and
    -- keep going.
    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

    GP=F and trace("[DEBUG]:<%s:%s>Looking at subrec digest(%s) Map(%s) L(%s)",
      MOD, meth, stringDigest, tostring(coldDirMap),tostring(digestList));

    coldDirRecDigest = coldDirMap[CDM_NextDirRec]; -- Next in Linked List.
    GP=F and trace("[DEBUG]:<%s:%s>Getting Next Digest in Dir Chain(%s)",
      MOD, meth, coldDirRecDigest );

    aerospike:close_subrec( coldDirRec );

  end -- while Dir Page not empty.

  GP=F and trace("[DEBUG]<%s:%s>After ColdListRead:LsoMap(%s) ColdHeadMap(%s)",
      MOD, meth, tostring( lsoMap ), tostring( coldDirMap )); 

  GP=F and trace("[EXIT]:<%s:%s>totalAmountRead(%d) ResultListSummary(%s) ",
      MOD, meth, totalNumRead, summarizeList(resultList));
  return totalNumRead;
end -- coldListRead()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO General Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- General Functions that require use of many of the above functions, so
-- they cannot be shoved into any one single category.
-- ======================================================================


-- ======================================================================
-- warmListTransfer()
-- ======================================================================
-- Transfer some amount of the WarmDigestList contents (the list of LSO Data
-- Record digests) into the Cold List, which is a linked list of Cold List
-- Directory pages that each point to a list of LDRs.
--
-- There is a configuration parameter (kept in the LSO Control Bin) that 
-- tells us how much of the warm list to migrate to the cold list. That
-- value is set at LSO Create time.
--
-- There is a lot of complexity at this level, as a single Warm List
-- transfer can trigger several operations in the cold list (see the
-- function makeRoomInColdList( lso, digestCount )
-- Parms:
-- (*) topRec: The top level user record (needed for create_subrec)
-- (*) lsoList
-- Return: Success (0) or Failure (-1)
-- ======================================================================
local function warmListTransfer( topRec, lsoList )
  local meth = "warmListTransfer()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s><>>> TRANSFER TO COLD LIST<<<<> lsoMap(%s)",
    MOD, meth, tostring(lsoMap) );

  -- if we haven't yet initialized the cold list, then set up the
  -- first Directory Head (a page of digests to data pages).  Note that
  -- the data pages are already built from the warm list, so all we're doing
  -- here is moving the reference (the digest value) from the warm list
  -- to the cold directory page.

  -- Build the list of items (digests) that we'll be moving from the warm
  -- list to the cold list. Use coldListInsert() to insert them.
  local transferList = extractWarmListTransferList( lsoList );
  rc = coldListInsert( topRec, lsoMap, transferList );
  GP=F and trace("[EXIT]: <%s:%s> lsoMap(%s) ", MOD, meth, tostring(lsoMap) );
  return rc;
end -- warmListTransfer()


-- ======================================================================
-- local function hotListTransfer( lsoList, insertValue )
-- ======================================================================
-- The job of hotListTransfer() is to move part of the HotList, as
-- specified by HotListTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmDigestList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
local function hotListTransfer( topRec, lsoList )
  local meth = "hotListTransfer()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> LSO Summary(%s) ",
      MOD, meth, tostring( lsoSummary(lsoList) ));
      --
  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmListHasRoom( lsoMap ) == 0 then
    warmListTransfer( topRec, lsoList );
  end

  -- Do this the simple (more expensive) way for now:  Build a list of the
  -- items (data entries) that we're moving from the hot list to the warm dir,
  -- then call insertWarmDir() to find a place for it.
  local transferList = extractHotListTransferList( lsoMap );
  rc = warmListInsert( topRec, lsoList, transferList );

  GP=F and trace("[EXIT]: <%s:%s> result(%d) LsoMap(%s) ",
    MOD, meth, rc, tostring( lsoMap ));
  return rc;
end -- hotListTransfer()
-- ======================================================================
--
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
end -- validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateRecBinAndMap( topRec, lsoBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=F and trace("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( lsoBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  validateBinName( lsoBinName );

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
    if( topRec[lsoBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSO BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(lsoBinName) );
      error('LSO BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local lsoList = topRec[lsoBinName]; -- The main lsoMap structure
    -- Extract the property map and lso control map from the lso bin list.
    local propMap = lsoList[1];
    local lsoMap  = lsoList[2];

    if propMap[PM_Magic] ~= MAGIC then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LSO BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( lsoBinName ) );
      error('LSO BIN Is Corrupted (No Magic::1)');
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[lsoBinName] ~= nil then
      local lsoList = topRec[lsoBinName]; -- The main lsoMap structure
      -- Extract the property map and lso control map from the lso bin list.
      local propMap = lsoList[1];
      local lsoMap  = lsoList[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LSO BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( lsoBinName ) );
        error('LSO BIN Is Corrupted (No Magic::2)');
      end
    end -- if worth checking
  end -- else for must exist

end -- validateRecBinAndMap()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSTACK Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || lstack_create ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- For this version of lstack, we will be using a SINGLE MAP object,
-- which contains lots of metadata, plus one list:
-- (*) Namespace Name (just one Namespace -- for now)
-- (*) Set Name
-- (*) Chunk Size (same for both namespaces)
-- (*) Warm Chunk Count: Number of Warm Chunk Data Records
-- (*) Cold Chunk Count: Number of Cold Chunk Data Records
-- (*) Item Count (will NOT be tracked in Stoneman)
-- (*) The List of Warm Chunks of data (each Chunk is a list)
-- (*) The Head of the Cold Data Directory
-- (*) Storage Mode (Compact or Regular) (0 for compact, 1 for regular)
-- (*) Compact Item List
--
-- The LSO starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record -- in the Hot List.  Once the
-- Hot List overflows, the entries flow into the warm list, which is a
-- list of LSO Data Records (each 2k record holds N values, where N is
-- approximately (2k/rec size) ).
-- Once the data overflows the warm list, it flows into the cold list,
-- which is a linked list of directory pages -- where each directory page
-- points to a list of LSO Data Record pages.  Each directory page holds
-- roughly 100 page pointers (assuming a 2k page).
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) createSpec: The map (not list) of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
--
--  NOTE: 
--  !!!! More parms needed here to appropriately configure the LSO
--  -> Package (one of the pre-named packages that hold all the info)
--  OR
--  Individual entries (this is now less attractive)
--  -> Hot List Size
--  -> Hot List Transfer amount
--  -> Warm List Size
--  -> Warm List Transfer amount
-- ========================================================================
function lstack_create( topRec, lsoBinName, createSpec )
  local meth = "stackCreate()";

  if createSpec == nil then
    GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s) NULL createSpec",
      MOD, meth, tostring(lsoBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> lsoBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( lsoBinName), tostring( createSpec ));
  end

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, false );

  -- Create and initialize the LSO Object:: The List that holds both
  -- the Property Map and the LSO Map;
  -- NOTE: initializeLso() also assigns the lsoList to the record bin.
  local lsoList = initializeLso( topRec, lsoBinName );

  -- Set the type of this record to LDT (it might already be set)
  record.set_type( topRec, RT_LDT ); -- LDT Type Rec

  -- If the user has passed in settings that override the defaults
  -- (the createSpec), then process that now.
  if createSpec ~= nil then
    adjustLsoList( lsoList, createSpec )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:LsoList after Init(%s)",
    MOD, meth, tostring(lsoList));

  -- Update the Record.
  topRec[lsoBinName] = lsoList;

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
end -- function lstack_create()
-- ======================================================================

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local stackPush (with implicit create)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Push a value onto the stack.
-- Also, if the LSO Bin does not yet exist, it will be created with the
-- values defined by the "package" name, if specified in the create_spec,
-- otherwise it will be created with the default values.
--
-- Regarding push(). There are different cases, with different
-- levels of complexity:
-- (*) HotListInsert: Instant: Easy
-- (*) WarmListInsert: Result of HotList Overflow:  Medium
-- (*) ColdListInsert: Result of WarmList Overflow:  Complex
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) newValue: The value to be inserted (pushed on the stack)
-- (4) createSpec: The map of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- NOTE: When using info/trace calls, ALL parameters must be protected
-- with "tostring()" so that we do not encounter a format error if the user
-- passes in nil or any other incorrect value/type.
-- ======================================================================
-- =======================================================================
local function localStackPush( topRec, lsoBinName, newValue, createSpec )
  local meth = "localStackPush()";

  -- Note: functionTable is "global" to this module, defined at top of file.

  GP=F and trace("[ENTER1]:<%s:%s>LSO BIN(%s) NewVal(%s) createSpec(%s)",
      MOD, meth, tostring(lsoBinName), tostring( newValue ),
      tostring( createSpec ) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, false );

  -- Check for existence, and create if not there.  If we create AND there
  -- is a "createSpec", then configure this LSO appropriately.
  local lsoList;
  local lsoMap;
  local propMap;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[WARNING]:<%s:%s>:Record Does Not exist. Creating",
      MOD, meth );
    lsoList = initializeLso( topRec, lsoBinName );
    if( createSpec ~= nil ) then
      adjustLsoList( lsoList, createSpec );
    end
    aerospike:create( topRec );
  elseif ( topRec[lsoBinName] == nil ) then
    GP=F and trace("[WARNING]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating",
                   MOD, meth, tostring(lsoBinName) );
    lsoList = initializeLso( topRec, lsoBinName );
    if( createSpec ~= nil ) then
      adjustLsoList( lsoList, createSpec );
    end
    aerospike:create( topRec );
  else
    -- if the map already exists, then we don't adjust with createSpec.
    lsoList = topRec[lsoBinName];
  end
  -- Extract the Property Map and LsoMap from the LsoList;
  propMap = lsoList[1];
  lsoMap  = lsoList[2];
  
  -- Now, it looks like we're ready to insert.  If there is a transform
  -- function present, then apply it now.
  -- Note: functionTable is "global" to this module, defined at top of file.
  local newStoreValue;
  if lsoMap[M_Transform] ~= nil  then 
    GP=F and trace("[DEBUG]: <%s:%s> Applying Transform (%s)",
      MOD, meth, tostring(lsoMap[M_Transform] ) );
    newStoreValue = functionTable[lsoMap[M_Transform]]( newValue );
  else
    newStoreValue = newValue;
  end

  -- If we have room, do the simple list insert.  If we don't have
  -- room, then make room -- transfer half the list out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.  (Ok to use lsoMap and not lsoList here).
  if hotListHasRoom( lsoMap, newStoreValue ) == false then
    GP=F and trace("[DEBUG]:<%s:%s>: CALLING TRANSFER HOT LIST!!",MOD, meth );
    hotListTransfer( topRec, lsoList );
  end
  hotListInsert( lsoList, newStoreValue );
  -- Must always assign the object BACK into the record bin.
  -- Check to see if we really need to reassign the MAP into the list as well.
  lsoList[2] = lsoMap;
  topRec[lsoBinName] = lsoList;

  -- All done, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", MOD, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc
end -- function localStackPush()

-- =======================================================================
-- Stack Push -- with and without implicit create spec.
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- =======================================================================
function lstack_push( topRec, lsoBinName, newValue )
  return localStackPush( topRec, lsoBinName, newValue, nil )
end -- end lstack_push()

function lstack_create_and_push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec );
end -- lstack_create_and_push()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local StackPeek: 
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return "peekCount" values from the stack, in Stack (LIFO) order.
-- For Each Bin (in LIFO Order), read each Bin in reverse append order.
-- If "peekCount" is zero, then return all.
-- Depending on "peekcount", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotList), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) newValue: The value to be inserted (pushed on the stack)
-- (4) func: The "Inner UDF" that will filter Peek output
-- (5) fargs: Arg List to the filter function (i.e. func(val, fargs)).
-- Result:
--   res = (when successful) List (empty or populated) 
--   res = (when error) nil
-- Note 1: We need to switch to a two-part return, with the first value
-- being the status return code, and the second being the content (or
-- error message).
--
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ======================================================================
local function localStackPeek( topRec, lsoBinName, peekCount, func, fargs )
  local meth = "localStackPeek()";

  GP=F and trace("[ENTER]: <%s:%s> LSO BIN(%s) Count(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(lsoBinName), tostring(peekCount),
    tostring(func), tostring(fargs) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, true );
  local lsoList = topRec[ lsoBinName ];
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  GP=F and trace("[DEBUG]: <%s:%s> LSO List Summary(%s)",
    MOD, meth, lsoSummaryString( lsoList ) );

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  
  -- When the user passes in a "peekCount" of ZERO, then we read ALL.
  -- Actually -- we will also read ALL if count is negative.
  local all = false;
  local count = 0;
  if peekCount <= 0 then
    all = true;
  else
    count = peekCount;
  end

  -- Set up our answer list.
  local resultList = list(); -- everyone will fill this in

  -- Fetch from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local resultList = hotListRead(resultList, lsoList, count, func, fargs, all);
  local numRead = list.size( resultList );
  GP=F and trace("[DEBUG]: <%s:%s> HotListResult:Summary(%s)",
      MOD, meth, summarizeList(resultList));

  local warmCount = 0;

  -- If the list had all that we need, then done.  Return list.
  if(( numRead >= count and all == false) or numRead >= propMap[PM_ItemCount] )
  then
    return resultList;
  end

  -- We need more -- get more out of the Warm List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot List.
  local remainingCount = 0; -- Default, when ALL flag is on.
  if( all == false ) then
    remainingCount = count - numRead;
  end
  GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d) All(%s)",
    MOD, meth, remainingCount, tostring(all));
  -- If no Warm List, then we're done (assume no cold list if no warm)
  if list.size(lsoMap[M_WarmDigestList]) > 0 then
    warmCount =
      warmListRead(topRec,resultList,lsoList,remainingCount,func,fargs,all);
  end

  -- As Agent Smith would say... "MORE!!!".
  -- We need more, so get more out of the COLD List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot and Warm Lists.
  local coldCount = 0;
  if( all == false ) then
    remainingCount = count - numRead - warmCount;
      GP=F and trace("[DEBUG]:<%s:%s>After WmRd:A(%s)RC(%d)PC(%d)NR(%d)WC(%d)",
        MOD, meth, tostring(all), remainingCount, count, numRead, warmCount );
  end

  GP=F and trace("[DEBUG]:<%s:%s>After WarmListRead: lsoMap(%s) lsoList(%s)",
    MOD, meth, tostring(lsoMap), lsoSummaryString(lsoList));

  numRead = list.size( resultList );
  -- If we've read enough, then return.
  if ( (remainingCount <= 0 and all == false) or
       (numRead >= propMap[PM_ItemCount] ) )
  then
      return resultList; -- We have all we need.  Return.
  end

  -- Otherwise, go look for more in the Cold List.
  local coldCount = 
      coldListRead(topRec,resultList,lsoList,remainingCount,func,fargs,all);

  GP=F and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    MOD, meth, peekCount, summarizeList(resultList));

  return resultList;
end -- function localStackPeek() 

-- =======================================================================
-- lstack_peek() -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- =======================================================================
function lstack_peek( topRec, lsoBinName, peekCount )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- end lstack_peek()

function lstack_peek_then_filter( topRec, lsoBinName, peekCount, func, fargs )
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- lstack_peek_then_filter()


-- ========================================================================
-- lstack_trim() -- Remove all but the top N elements
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) trimCount: Leave this many elements on the stack
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack_trim( topRec, lsoBinName, trimCount )
  local meth = "lstack_trim()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s) trimCount(%s)",
    MOD, meth, tostring(lsoBinName), tostring( trimCount ));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  error('Trim() is not yet implemented (Sorry)');

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );

  return config;
end -- function lstack_trim()

-- ========================================================================
-- lstack_size() -- return the number of elements (item count) in the stack.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the size)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack_size( topRec, lsoBinName )
  local meth = "lstack_size()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local lsoList = topRec[ lsoBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local itemCount = propMap[PM_ItemCount];

  GP=F and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function lstack_size()

-- ========================================================================
-- lstack_config() -- return the config settings
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = (when successful) config Map 
--   res = (when error) nil
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack_config( topRec, lsoBinName )
  local meth = "lstack_config()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local lsoList = topRec[ lsoBinName ];
  local config = lsoSummary( lsoList );

  GP=F and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lstack_config()


-- ========================================================================
-- lstack_subrec_list() -- Return a list of subrecs
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack_subrec_list( topRec, lsoBinName )
  local meth = "lstack_subrec_list()";

  GP=F and trace("[ENTER]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  local lsoList = topRec[ lsoBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- Copy the warm list into the result list
  local wdList = lsoMap[M_WarmDigestList];
  local transAmount = list.size( wdList );
  local resultList = list.take( wdList, transAmount );

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(lsoMap[M_ColdDirListHead] == nil or lsoMap[M_ColdDirListHead] == 0) then
    return resultList;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = lsoMap[M_ColdDirListHead];

  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    -- Save the Dir Digest
    list.append( resultList, coldDirRecDigest );

    -- Open the Directory Page, read the digest list
    local stringDigest = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = aerospike:open_subrec( topRec, stringDigest );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    for i = 1, list.size(digestList), 1 do 
      list.append( resultList, digestList[i] );
    end

    -- Get the next Cold Dir Node in the list
    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
    coldDirRecDigest = coldDirMap[CDM_NextDirRec]; -- Next in Linked List.
    -- If no more, we'll drop out of the loop, and if there's more, 
    -- we'll get it in the next round.
    -- Close this directory subrec before we open another one.
    aerospike:close_subrec( coldDirRec );

  end -- Loop thru each cold directory

  GP=F and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList

end -- lstack_subrec_list()


-- ========================================================================
-- lstack_delete() -- Delete the entire lstack
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function lstack_delete( topRec, lsoBinName )
  local meth = "lstack_delete()";

  GP=F and trace("[ENTER]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  local rc = 0; -- start off optimistic

  -- Validate the lsoBinName before moving forward
  validateRecBinAndMap( topRec, lsoBinName, true );

  -- Get the lsoMap from the topRec.
  local lsoMap = topRec[lsoBinName];

  trace("[ATTENTION!!!]::LSTACK_DELETE IS NOT YET IMPLEMENTED!!!");

  return rc;

end -- lstack_delete()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
