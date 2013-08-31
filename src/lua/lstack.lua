-- Large Stack Object (LSO or LSTACK) Operations
-- lstack.lua:  August 26, 2013
--
-- Module Marker: Keep this in sync with the stated version
local MOD="lstack_2013_08_29.d"; -- the module name used for tracing

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.1;

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
-- LSTACK TODO LIST:
-- Future: Short, Medium and Long Term
-- Priority: High, Medium and Low
-- Difficulty: High, Medium and Low
-- ======================================================================
-- ACTIVITIES LIST:
-- (*) DONE: (Started: July 23, 2012)
--   + Release Cold Storage:  Release an entire Cold Dir Page (with the list
--     of subrec digests) on addition of a new Cold-Head, when the Cold Dir
--     Count > Max.
--   + Also, change the Cold Dir Pages to be doubly linked rather than singly
--     linked.
--   + Add a Cold Dir TAIL pointer in addition to the HEAD pointer -- so that
--     releasing the oldest Dir Rec Page (and its children) is quicker/easier.
--
-- (*) DONE: (Started: July 23, 2012)
--   + Implement the subrecContext (subrec management) here in LSTACK that
--     we have in LLIST.  Name them all ldt_subrecXXXX() because they
--     will become ldt common methods.
--
-- (*) HOLD: (Started: July 23, 2012)
--   + Switch to ldt_common.lua for the newly common functions.
--   + Put on hold until next release work.
--
-- (*) IN-DONE: (Started: July 23, 2012)
--   + Added new method: lstack_set_storage_limit(), which limits peek sizes
--     and also sets/resets the storage parameter values so that item counts
--     over the size limits (Hot, Warm or Cold list) will discard old data.
--
-- (*) IN-PROGRESS: (Started: July 25, 2012)
--   + Add new "crec_release" method that takes a LIST of digests and
--     releases the storage.  Raj will fill in the C code on the server
--     side does the delete.
--
-- TODO:
-- (+) HOLD:  Trim is less important than "set_storage_limit()", and is
--     more expensive.
--     Implement Trim (release LDR pages from Warm/Cold List)
--     stack_trim(): Must release storage before record delete.
--     Notice that this is lower priority now that we're going to look to
--     the Cold List Storage Release to deal with storage reclaimation
--     from lstack eviction.
--     F:S, P:M, D:M
-- (+) Add a LIMIT value to the control map -- and when we are a page over
--     then release the page (ideally -- do not slow down transaction to
--     do this.  Can we add a task to a cleanup thread for this?)
--     Answers:
--     (1) We set a new MAP value (M_StoreLimit) so that we never read
--     past the end.
--     (2) We check the storage state on ColdList Insert to see if we're
--     past the end -- and if so -- we release any LDRs (and thus Cold Dirs)
--     that are holding "Frozen data" that is beyond the end. Note that
--     we could eventually save "Frozen Data" in a file for permanent
--     archive, if we so desired.  That could also be done for a background
--     task.
--     (3) Define the SearchPath object for LSTACK to find a position,
--     then let other functions do things relative to that position, such
--     as release storage.
-- (+) NOTICE: Position Calc is LIST MODE ONLY::
--     : Must be extended for BINARY MODE.
--   These must be kept current:  M_WarmTopEntryCount, M_WarmTopByteCount      
--   They will replace WarmTopFul with an actual count.
--
-- ======================================================================
-- DONE:
-- (*) Implement LDT Remove -- remove the ESR and the Bin Contents, and
--     then let the NSUP/Defrag mechanism clean up the subrecs.
-- (*) ldtInitPropMap() method
-- (*) Init the record Prop Bin on first LDT Create
-- (*) Add Bin Flags, and Record Types (record.set_flags(), set_type())
-- (*) Add ESR:  Create the ESR on first SubRec Create
-- (*) Add a Record-level Hidden Bin (holds Record Version Info, LDT Count)
-- (*) Switch to ldtCtrl (PropMap, ldtMap):: Common Property Map
-- (*) LStack Peek with filters.
-- (*) LStack Peek with Transform/Untransform
-- (*) LStack Packages to define sets of parameters
-- (*) Main LStack Functions (push, peek:: Simple, Complex)
--
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
-- (*) create: Create the LSO structure in the chosen topRec bin
-- (*) push: Push a user value (AS_VAL) onto the stack
-- (*) create_and_push: Push a user value (AS_VAL) onto the stack
-- (*) peek: Read N values from the stack, in LIFO order
-- (*) peek_then_filter: Read N values from the stack, in LIFO order
-- (*) trim: Release all but the top N values.
-- (*) remove: Release all storage related to this lstack object
-- (*) config: retrieve all current config settings in map format
-- (*) size: Report the NUMBER OF ITEMS in the stack.
-- (*) subrec_list: Return the list of subrec digests
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
--  |                                            
--  |                           <Newest Dir............Oldest Dir>
--  +-------------------------->+-----+->+-----+->+-----+-->+-----+-+
--   (DirRec Pages DoubleLink)<-+Rec  |<-+Rec  |<-+Rec  | <-+Rec  | V
--    The cold dir is a linked  |Chunk|  |Chunk|  |Chunk| o |Chunk|
--    list of dir pages that    |Dir  |  |Dir  |  |Rec  | o |Dir  |
--    point to LSO Data Records +-----+  +-----+  +-----+   +-----+
--    that hold the actual cold [][]:[]  [][]:[]  [][]:[]   [][]:[]
--    data (cold chunks).       +-----+  +-----+  +-----+   +-----+
--                               | |  |   | |  |   | |  |    | |  |
--    LDRS (per dir) have age:   | |  V   | |  V   | |  V    | |  V
--    <Oldest LDR .. Newest LDR> | |::+--+| |::+--+| |::+--+ | |::+--+
--    As "Warm Data" ages out    | |::|Cn|| |::|Cn|| |::|Cn| | |::|Cn|
--    of the Warm Dir List, the  | V::+--+| V::+--+| V::+--+ | V::+--+
--    LDRs transfer out of the   | +--+   | +--+   | +--+    | +--+
--    Warm Directory and into    | |C2|   | |C2|   | |C2|    | |C2|
--    the cold directory.        V +--+   V +--+   V +--+    V +--+
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
-- NOTES:
-- Design, V3.x.  For really cold data -- things out beyond 50,000
-- elements, it might make sense to just push those out to a real disk
-- based file (to which we could just append -- and read in reverse order).
-- If we ever need to read the whole stack, we can afford
-- the time and effort to read the file (it is an unlikely event).  The
-- issue here is that we probably have to teach Aerospike how to transfer
-- (and replicate) files as well as records.
--
-- Design, V3.x. We will need to limit the amount of data that is held
-- in a stack. We've added "StoreLimit" to the ldtMap, as a way to limit
-- the number of items.  Note that this can be used to limit both the
-- storage and the read amounts.
-- One way this could be used is to REUSE a cold LDR page when an LDR
-- page is about to fall off the end of the cold list.  However, that
-- must be considered carefully -- as the time and I/O spent messing
-- with the cold directory and the cold LDR could be a performance hit.
-- We'll have to consider how we might age these pages out gracefully
-- if we can't cleverly reuse them (patent opportunity here).
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
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
-- Aerospike Server Functions:
-- ======================================================================
-- Aerospike Record Functions:
-- status = aerospike:create( topRec )
-- status = aerospike:update( topRec )
-- status = aerospike:remove( rec ) (not currently used)
--
--
-- Aerospike SubRecord Functions:
-- newRec = aerospike:create_subrec( topRec )
-- rec    = aerospike:open_subrec( topRec, childRecDigest)
-- status = aerospike:update_subrec( childRec )
-- status = aerospike:close_subrec( childRec )
-- status = aerospike:remove_subrec( subRec )  
--
-- Record Functions:
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

-- Default storage limit for a stack -- can be overridden by setting
-- one of the packages.
local G_STORE_LIMIT = 20000  -- Store no more than this.  User can override.

-- ++==================++
-- || External Modules ||
-- ++==================++
-- Get addressability to the Function Table: Used for compress and filter
local functionTable = require('UdfFunctionTable');
-- Common LDT functions that are used by ALL of the LDTs.
-- local LDTC = require('ldt_common');
local ldte=require('ldt_errors');

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

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

-- LDT TYPES (only lstack is defined here)
local LDT_TYPE_LSTACK = "LSTACK";

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

-- We maintain a pool, or "context", of subrecords that are open.  That allows
-- us to look up subrecs and get the open reference, rather than bothering
-- the lower level infrastructure.  There's also a limit to the number
-- of open subrecs.
local G_OPEN_SR_LIMIT = 20;

-- When the user wants to override the default settings, or register some
-- functions, the user module with the "adjust_settings" function will be
-- used.
local G_SETTINGS = "adjust_settings";

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

-- The Cold Dir Records use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
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
local PackageDebugModeObject     = "DebugModeObject";
local PackageDebugModeObjectDups = "DebugModeObjectDups";
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
--   + All Field names (e.g. ldtMap[StoreMode]) begin with Upper Case
--   + All variable names (e.g. ldtMap[StoreMode]) begin with lower Case
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
--     That means that the "ldtMap" that was the primary value in the LsoBin
--     is now a list, where ldtCtrl[1] will always be the propMap and
--     ldtCtrl[2] will always be the ldtMap.  In the server code, using "C",
--     we will sometimes read the ldtCtrl[1] (the property map) in order to
--     perform some LDT management operations.
--   + Since Lua wraps up the LDT Control map as a self-contained object,
--     we are paying for storage in EACH LDT Bin for the map field names. 
--     Thus, even though we like long map field names for readability:
--     e.g.  ldtMap.HotEntryListItemCount, we don't want to spend the
--     space to store the large names in each and every LDT control map.
--     So -- we do another Lua Trick.  Rather than name the key of the
--     map value with a large name, we instead use a single character to
--     be the key value, but define a descriptive variable name to that
--     single character.  So, instead of using this in the code:
--     ldtMap.HotEntryListItemCount = 50;
--            123456789012345678901
--     (which would require 21 bytes of storage); We instead do this:
--     local HotEntryListItemCount='H';
--     ldtMap[HotEntryListItemCount] = 50;
--     Now, we're paying the storage cost for 'H' (1 byte) and the value.
--
--     So -- we have converted all of our LDT lua code to follow this
--     convention (fields become variables the reference a single char)
--     and the mapping of long name to single char will be done in the code.
-- ------------------------------------------------------------------------
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
local PM_ItemCount             = 'I'; -- (Top): # of items in LDT
local PM_SubRecCount           = 'S'; -- (Top): # of subrecs in the LDT
local PM_Version               = 'V'; -- (Top): Code Version
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_CreateTime            = 'C'; -- (All): Creation time of this rec
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
local CDM_PrevDirRec           = 'P';-- Ptr to Prev Cold Dir Page
local CDM_DigestCount          = 'C';-- Current Digest Count
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local M_StoreMode              = 'M'; -- List or Binary Mode
local M_StoreLimit             = 'S'; -- Max Item Count for stack
local M_UserModule             = 'U'; -- Name of the User Module
local M_Transform              = 't'; -- User's Transform function
local M_UnTransform            = 'u'; -- User's UNTransform function
local M_LdrEntryCountMax       = 'e'; -- Max # of entries in an LDR
local M_LdrByteEntrySize       = 's'; -- Fixed Size of a binary Object in LDR
local M_LdrByteCountMax        = 'b'; -- Max # of bytes in an LDR
local M_HotEntryList           = 'H'; -- The Hot Entry List
local M_HotEntryListItemCount  = 'L'; -- The Hot List Count
local M_HotListMax             = 'h'; -- Max Size of the Hot List
local M_HotListTransfer        = 'X'; -- Amount to transfer from Hot List
local M_WarmDigestList         = 'W'; -- The Warm Digest List
local M_WarmListDigestCount    = 'l'; -- # of Digests in the Warm List
local M_WarmListMax            = 'w'; -- Max # of Digests in the Warm List
local M_WarmListTransfer       = 'x'; -- Amount to Transfer from the Warm List
-- Note that WarmTopXXXXCount will eventually replace the need to show if
-- the Warm Top is FULL -- because we'll always know the count (and "full"
-- will be self-evident).
local M_WarmTopFull            = 'F'; -- Boolean: Shows if Warm Top is full
local M_WarmTopEntryCount      = 'A'; -- # of Objects in the Warm Top (LDR)
local M_WarmTopByteCount       = 'a'; -- # Bytes in the Warm Top (LDR)

-- Note that ColdTopListCount will eventually replace the need to know if
-- the Cold Top is FULL -- because we'll always know the count of the Cold
-- Directory Top -- and so "full" will be self-evident.
local M_ColdTopFull            = 'f'; -- Boolean: Shows if Cold Top is full
local M_ColdTopListCount       = 'T'; -- Shows List Count for Cold Top

local M_ColdDirListHead        = 'Z'; -- Digest of the Head of the Cold List
local M_ColdDirListTail        = 'z'; -- Digest of the Head of the Cold List
local M_ColdDataRecCount       = 'R';-- # of LDRs in Cold Storage
-- It's assumed that this will match the warm list size, and we'll move
-- half of the warm digest list to a cold list on each transfer.
local M_ColdListMax            = 'c';-- Max # of items in a cold dir list
-- This is used to LIMIT the size of an LSTACK -- we will do it efficiently
-- at the COLD DIR LEVEL.  So, for Example, if we set it to 3, then we'll
-- discard the last (full) cold Dir List when we add a new fourth Dir Head.
-- Thus, the number of FULL Cold Directory Pages "D" should be set at
-- (D + 1).
local M_ColdDirRecMax          = 'C';-- Max # of Cold Dir subrecs we'll have
local M_ColdDirRecCount        = 'r';-- # of Cold Dir sub-Records

-- ------------------------------------------------------------------------
-- Maintain the LSO letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
--
-- A:M_WarmTopEntryCount      a:M_WarmTopByteCount      0:
-- B:                         b:M_LdrByteCountMax       1:
-- C:M_ColdDirRecMax          c:M_ColdListMax           2:
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
-- S:M_StoreLimit             s:M_LdrByteEntrySize
-- T:M_ColdTopListCount       t:M_Transform
-- U:M_UserModule             u:M_UnTransform
-- V:                         v:
-- W:M_WarmDigestList         w:M_WarmListMax
-- X:M_HotListTransfer        x:M_WarmListTransfer
-- Y:                         y:
-- Z:M_ColdDirListHead        z:M_ColdDirListTail
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

-- ======================================================================
-- local function lsoSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function lsoSummary( ldtCtrl )
  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    return "EMPTY LDT BIN VALUE";
  end

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  if( propMap[PM_Magic] ~= MAGIC ) then
    return "BROKEN MAP--No Magic";
  end;

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();

  -- Properties
  resultMap.SUMMARY              = "LStack Summary";
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropSubRecCount      = propMap[PM_SubRecCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  resultMap.PropCreateTime       = propMap[PM_CreateTime];
  
  -- General LSO Parms:
  resultMap.StoreMode            = ldtMap[M_StoreMode];
  resultMap.StoreLimit           = ldtMap[M_StoreLimit];
  resultMap.UserModule           = ldtMap[M_UserModule];
  resultMap.Transform            = ldtMap[M_Transform];
  resultMap.UnTransform          = ldtMap[M_UnTransform];

  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = ldtMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[M_LdrByteCountMax];
  --
  -- Hot Entry List Settings: List of User Entries
  resultMap.HotListMax            = ldtMap[M_HotListMax];
  resultMap.HotListTransfer       = ldtMap[M_HotListTransfer];
  resultMap.HotEntryListItemCount = ldtMap[M_HotEntryListItemCount];

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  resultMap.WarmListMax           = ldtMap[M_WarmListMax];
  resultMap.WarmListTransfer      = ldtMap[M_WarmListTransfer];
  resultMap.WarmListDigestCount   = ldtMap[M_WarmListDigestCount];

  -- Cold Directory List Settings: List of Directory Pages
  resultMap.ColdDirListHead       = ldtMap[M_ColdDirListHead];
  resultMap.ColdListMax           = ldtMap[M_ColdListMax];
  resultMap.ColdDirRecMax         = ldtMap[M_ColdDirRecMax];
  resultMap.ColdListDirRecCount   = ldtMap[M_ColdListDirRecCount];
  resultMap.ColdListDataRecCount  = ldtMap[M_ColdListDataRecCount];
  resultMap.ColdTopFull           = ldtMap[M_ColdTopFull];
  resultMap.ColdTopListCount      = ldtMap[M_ColdTopListCount];

  return resultMap;
end -- lsoSummary()

-- ======================================================================
-- Make it easier to use lsoSummary(): Have a String version.
-- ======================================================================
local function lsoSummaryString( ldtCtrl )
  return tostring( lsoSummary( ldtCtrl ) );
end

-- =============================
-- Begin SubRecord Function Area (MOVE THIS TO LDT_COMMON)
-- =============================
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, will employ the "subrecContext" object, which
-- will hold all of the subrecords that were opened during processing. 
-- Note that some operations can potentially involve many subrec
-- operations -- and can also potentially revisit pages.
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

  -- We need to track BOTH the Open Records and their Dirty State.
  -- Do this with a LIST of maps:
  -- recMap   = srcList[1]
  -- dirtyMap = srcList[2]

  -- Code not yet changed.
  local srcList = list();
  local recMap = map();
  local dirtyMap = map();
  recMap.ItemCount = 0;
  list.append( srcList, recMap ); -- recMap
  list.append( srcList, dirtyMap ); -- dirtyMap

  GP=E and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return srcList;
end -- createSubrecContext()

-- ======================================================================
-- Given an already opened subrec (probably one that was recently created),
-- add it to the subrec context.
-- ======================================================================
local function addSubrecToContext( srcList, subrec )
  local meth = "addSubrecContext()";
  GP=E and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring( srcList));

  if( srcList == nil ) then
    warn("[ERROR]<%s:%s> Bad Subrec Context: SRC is NIL", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  local digest = record.digest( subrec );
  local digestString = tostring( digest );
  recMap[digestString] = subrec;

  local itemCount = recMap.ItemCount;
  recMap.ItemCount = itemCount + 1;

  GP=E and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- openSubrec()
-- ======================================================================
local function openSubrec( srcList, topRec, digestString )
  local meth = "openSubrec()";
  GP=E and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
    MOD, meth, tostring(topRec), digestString, tostring(srcList));

  -- We have a global limit on the number of subrecs that we can have
  -- open at a time.  If we're at (or above) the limit, then we must
  -- exit with an error (better here than in the subrec code).
  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local itemCount = recMap.ItemCount;

  local subrec = recMap[digestString];
  if( subrec == nil ) then
    if( itemCount >= G_OPEN_SR_LIMIT ) then
      warn("[ERROR]<%s:%s> SRC Count(%d) Exceeded Limit(%d)", MOD, meth,
        itemCount, G_OPEN_SR_LIMIT );
      error( ldte.ERR_TOO_MANY_OPEN_SUBRECS );
    end

    recMap.ItemCount = itemCount + 1;
    GP=F and trace("[OPEN SUBREC]<%s:%s>SRC.ItemCount(%d) TR(%s) DigStr(%s)",
      MOD, meth, recMap.ItemCount, tostring(topRec), digestString );
    subrec = aerospike:open_subrec( topRec, digestString );
    GP=F and trace("[OPEN SUBREC RESULTS]<%s:%s>(%s)", 
      MOD,meth,tostring(subrec));
    if( subrec == nil ) then
      warn("[ERROR]<%s:%s> Subrec Open Failure: Digest(%s)", MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
  else
    GP=F and trace("[FOUND REC]<%s:%s>Rec(%s)", MOD, meth, tostring(subrec));
  end

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
    MOD, meth, tostring(subrec), digestString );
  return subrec;
end -- openSubrec()


-- ======================================================================
-- closeSubrec()
-- ======================================================================
-- Close the subrecord -- providing it is NOT dirty.  For all dirty
-- subrecords, we have to wait until the end of the UDF call, as THAT is
-- when all dirty subrecords get written out and closed.
-- ======================================================================
local function closeSubrec( srcList, digestString )
  local meth = "closeSubrec()";
  GP=E and trace("[ENTER]<%s:%s> DigestStr(%s) SRC(%s)",
    MOD, meth, digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local itemCount = recMap.ItemCount;
  local rc = 0;

  local subrec = recMap[digestString];
  local dirtyStatus = dirtyMap[digestString];
  if( subrec == nil ) then
    warn("[INTERNAL ERROR]<%s:%s> Rec not found for Digest(%s)", MOD, meth,
      digestString );
    error( ldte.ERR_INTERNAL );
  end

  info("[STATUS]<%s:%s> Closing Rec: Digest(%s)", MOD, meth, digestString);

  if( dirtyStatus == true ) then
    warn("[WARNING]<%s:%s> Can't close Dirty Record: Digest(%s)",
      MOD, meth, digestString);
  else
    rc = aerospike:close_subrec( subrec );
    GP=F and trace("[STATUS]<%s:%s>Closed Rec: Digest(%s) rc(%s)", MOD, meth,
      digestString, tostring( rc ));
  end

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- closeSubrec()


-- ======================================================================
-- updateSubrec()
-- ======================================================================
-- Update the subrecord -- and then mark it dirty.
-- ======================================================================
local function updateSubrec( srcList, subrec, digest )
  local meth = "updateSubrec()";
  GP=E and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
    MOD, meth, tostring(topRec), digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local rc = 0;

  if( digest == nil or digest == 0 ) then
    digest = record.digest( subrec );
  end
  local digestString = tostring( digest );

  rc = aerospike:update_subrec( subrec );
  dirtyMap[digestString] = true;

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- updateSubrec()

-- ======================================================================
-- markSubrecDirty()
-- ======================================================================
local function markSubrecDirty( srcList, digestString )
  local meth = "markSubrecDirty()";
  GP=E and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  -- Pull up the dirtyMap, find the entry for this digestString and
  -- mark it dirty.  We don't even care what the existing value used to be.
  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  dirtyMap[digestString] = true;
  
  GP=E and trace("[EXIT]<%s:%s> SRC(%s)", MOD, meth, tostring(srcList) );
  return 0;
end -- markSubrecDirty()

-- ======================================================================
-- closeAllSubrecs()
-- ======================================================================
local function closeAllSubrecs( srcList )
  local meth = "closeAllSubrecs()";
  GP=E and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  -- Iterate thru the SubRecContext and close all subrecords.
  local digestString;
  local rec;
  local rc = 0;
  for name, value in map.pairs( recMap ) do
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
    local vinfo = 0;
    recPropMap[RPM_VInfo] = vinfo; 
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
  -- Set this control bin as HIDDEN
  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
  return rc;
end -- setLdtRecordType()


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
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
--     holds the Data entries.  -- (*) The HotListTransfer should be half or one quarter the size of the
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
-- ldtMap = 
-- local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
-- local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
-- local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
-- local RT_ESR = 4; -- 0x4: Existence Sub Record
-- ======================================================================
local function initializeLso( topRec, lsoBinName )
  local meth = "initializeLso()";
  GP=E and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific Lso Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local ldtMap = map();
  local ldtCtrl = list();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount] = 0; -- A count of all items in the stack
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LSTACK; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = lsoBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]    = 0; -- not set yet.
  propMap[PM_CreateTime] = aerospike:get_current_time();
  propMap[PM_SelfDigest] = record.digest( topRec );

  -- Specific LSO Parms: Held in LsoMap
  ldtMap[M_StoreMode]   = SM_LIST; -- SM_LIST or SM_BINARY:
  ldtMap[M_StoreLimit]  = G_STORE_LIMIT;  -- Store no more than this.

  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax]= 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[M_LdrByteEntrySize]=  0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax] =   0; -- Max # of Data Chunk Bytes (binary mode)

  -- Hot Entry List Settings: List of User Entries
  ldtMap[M_HotEntryList]         = list(); -- the list of data entries
  ldtMap[M_HotEntryListItemCount]=   0; -- Number of elements in the Top List
  ldtMap[M_HotListMax]           = 100; -- Max Number for the List(then xfer)
  ldtMap[M_HotListTransfer]      =  50; -- How much to Transfer at a time.

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  ldtMap[M_WarmDigestList]       = list(); -- the list of digests for LDRs
  ldtMap[M_WarmTopFull] = false; --true when top chunk is full(for next write)
  ldtMap[M_WarmListDigestCount]  = 0; -- Number of Warm Data Record Chunks
  ldtMap[M_WarmListMax]          = 100; -- Number of Warm Data Record Chunks
  ldtMap[M_WarmListTransfer]     = 2; -- Number of Warm Data Record Chunks
  ldtMap[M_WarmTopEntryCount]    = 0; -- Count of entries in top warm chunk
  ldtMap[M_WarmTopByteCount]     = 0; -- Count of bytes used in top warm Chunk

  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[M_ColdDirListHead]= 0; -- Head (Rec Digest) of the Cold List Dir Chain
  ldtMap[M_ColdTopFull]    = false; -- true when cold head is full (next write)
  ldtMap[M_ColdDataRecCount]= 0; -- # of Cold DATA Records (data chunks)
  ldtMap[M_ColdDirRecCount] = 0; -- # of Cold DIRECTORY Records
  ldtMap[M_ColdDirRecMax]   = 5; -- Max# of Cold DIRECTORY Records
  ldtMap[M_ColdListMax]     = 100; -- # of list entries in a Cold list dir node

  -- Put our new maps in a list, in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  topRec[lsoBinName]            = ldtCtrl;

  GP=F and trace("[DEBUG]: <%s:%s> : Lso Summary after Init(%s)",
      MOD, meth , lsoSummaryString(ldtCtrl));

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  setLdtRecordType( topRec );

  -- Set the BIN Flag type to show that this is an LDT Bin, with all of
  -- the special priviledges and restrictions that go with it.
  GP=F and trace("[DEBUG]:<%s:%s>About to call record.set_flags(Bin(%s)F(%s)",
    MOD, meth, lsoBinName, tostring(BF_LDT_BIN) );

  record.set_flags( topRec, lsoBinName, BF_LDT_BIN );

  GP=F and trace("[DEBUG]: <%s:%s> Back from calling record.set_flags()",
    MOD, meth );

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtCtrl;
end -- initializeLso()

-- ======================================================================
-- ldtInitPropMap( propMap, subDigest, topDigest, rtFlag, ldtMap )
-- ======================================================================
-- Set up the LDR Property Map (one PM per LDT).  This function will move
-- into the ldtCommon module.
-- Parms:
-- (*) propMap: 
-- (*) esrDigest:
-- (*) subDigest:
-- (*) topDigest:
-- (*) rtFlag:
-- (*) topPropMap;
-- ======================================================================
local function
ldtInitPropMap( propMap, esrDigest, selfDigest, topDigest, rtFlag, topPropMap )
  local meth = "ldtInitPropMap()";
  GP=E and trace("[ENTER]: <%s:%s>", MOD, meth );

  -- Remember the ESR in the Top Record
  topPropMap[PM_EsrDigest] = esrDigest;

  -- Initialize the PropertyMap in the new ESR
  propMap[PM_EsrDigest]    = esrDigest;
  propMap[PM_RecType  ]    = rtFlag;
  propMap[PM_Magic]        = MAGIC;
  propMap[PM_ParentDigest] = topDigest;
  propMap[PM_SelfDigest]   = selfDigest;

end -- ldtInitPropMap()
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
local function createAndInitESR(src, topRec, ldtCtrl )
  local meth = "createAndInitESR()";
  GP=E and trace("[ENTER]: <%s:%s>", MOD, meth );

  local rc = 0;

  -- Remember to add this to the SRC after it is initialized.
  local esrRec    = aerospike:create_subrec( topRec );

  if( esrRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating ESR", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  local esrDigest = record.digest( esrRec);
  local topDigest = record.digest( topRec );
  local topPropMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Set the Property ControlMap for the ESR, and assign the parent Digest
  -- Note that we use our standard convention for property maps - all subrecs
  -- have a property map.
  -- Init the properties map for this ESR. Note that esrDigest is in here
  -- twice -- once for "self" and once for "esrRec".
  local esrPropMap = map();
  ldtInitPropMap(esrPropMap,esrDigest,esrDigest,topDigest,RT_ESR,topPropMap);

  -- NOTE: We have to make sure that the TopRec propMap also gets saved.
  esrRec[SUBREC_PROP_BIN] = esrPropMap;

  
  -- Set the record type as "ESR"
  trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esrRec, RT_ESR );
  trace("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s> Leaving with ESR Digest(%s)",
    MOD, meth, tostring(esrDigest));

  -- Now that it's initialized, add the ESR to the SRC.
  addSubrecToContext( src, esrRec );

  rc = aerospike:update_subrec( esrRec );
  if( rc == nil or rc == 0 ) then
      aerospike:close_subrec( esrRec );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

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
local function initializeLdrMap(src,topRec,ldrRec,ldrPropMap,ldrMap,ldtCtrl)
  local meth = "initializeLdrMap()";
  GP=E and trace("[ENTER]: <%s:%s>", MOD, meth );

  local lsoPropMap = ldtCtrl[1];
  local ldtMap     = ldtCtrl[2];

  ldrPropMap[PM_RecType]      = RT_SUB;
  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  ldrPropMap[PM_SelfDigest]   = record.digest( ldrRec );
  --  Not doing Log stuff yet
  --  ldrPropMap[PM_LogInfo]      = lsoPropMap[M_LogInfo];

  --  Use Top level LSO entry for mode and max values
  ldrMap[LDR_ByteEntrySize]   = ldtMap[M_LdrByteEntrySize];
  ldrMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries

  -- If this is the first LDR, then it's time to create an ESR for this
  -- LDT.
  if( lsoPropMap[PM_EsrDigest] == nil or lsoPropMap[PM_EsrDigest] == 0 ) then
    lsoPropMap[PM_EsrDigest] = createAndInitESR(src,topRec, ldtCtrl );
  end

  local lsopropMap = ldtCtrl[1];
  ldrPropMap[PM_EsrDigest] = lsoPropMap[PM_EsrDigest];
  GP=F and trace("LDR MAP: [%s:%s:%s]", tostring(ldrPropMap[PM_SelfDigest]),
    tostring(ldrPropMap[PM_EsrDigest]), tostring(ldrPropMap[PM_ParentDigest]));

  -- Set the type of this record to LDT (it might already be set by another
  -- LDT in this same record).
  record.set_type( ldrRec, RT_SUB ); -- LDT Type Rec

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
-- (*) ldtCtrl
-- ======================================================================
local function initializeColdDirMap( topRec, cdRec, cdPropMap, cdMap, ldtCtrl )
  local meth = "initializeColdDirMap()";
  GP=E and trace("[ENTER]: <%s:%s>", MOD, meth );

  local lsoPropMap = ldtCtrl[1];
  local ldtMap     = ldtCtrl[2];
  
  cdPropMap[PM_RecType]      = RT_SUB;
  cdPropMap[PM_ParentDigest] = record.digest( topRec );
  cdPropMap[PM_SelfDigest] = record.digest( cdRec );

  cdMap[CDM_NextDirRec] = 0; -- no other Dir Records (yet).
  cdMap[CDM_PrevDirRec] = 0; -- no other Dir Records (yet).
  cdMap[CDM_DigestCount] = 0; -- no digests in the list -- yet.
  local lsopropMap = ldtCtrl[1];
  cdPropMap[PM_EsrDigest] = lsoPropMap[PM_EsrDigest];
  GP = F and trace("CD MAP: [%s:%s:%s]", tostring(cdPropMap[PM_SelfDigest]), tostring(cdPropMap[PM_EsrDigest]), tostring(cdPropMap[PM_ParentDigest]));

  -- Set the type of this record to LDT (it might already be set by another
  -- LDT in this same record).
  record.set_type( cdRec, RT_SUB ); -- LDT Type Rec
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
-- Package = "DebugModeObjectDups"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- Test with Objects and the General Range Filter Predicate
-- ======================================================================
local function packageDebugModeObjectDups( ldtMap )
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
end -- packageDebugModeObjectDups()

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
-- adjustLsoList:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LsoMap:
-- Parms:
-- (*) ldtCtrl: the main LSO Bin value (propMap, ldtMap)
-- (*) argListMap: Map of LSO Settings 
-- Return: The updated LsoList
-- ======================================================================
local function adjustLsoList( ldtCtrl, argListMap )
  local meth = "adjustLsoList()";
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  GP=E and trace("[ENTER]: <%s:%s>:: LsoList(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtCtrl), tostring( argListMap ));

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
          packageStandardList( ldtMap );
      elseif value == PackageTestModeList then
          packageTestModeList( ldtMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( ldtMap );
      elseif value == PackageProdListValBinStore then
          packageProdListValBinStore( ldtMap );
      elseif value == PackageDebugModeObject then
          packageDebugModeObject( ldtMap );
      elseif value == PackageDebugModeObjectDups then
          packageDebugModeObjectDups( ldtMap );
      elseif value == PackageDebugModeList then
          packageDebugModeList( ldtMap );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( ldtMap );
      end
    elseif name == "StoreMode" and type( value )  == "string" then
      -- Verify it's a valid value
      if value == SM_LIST or value == SM_BINARY then
        ldtMap[M_StoreMode] = value;
      end
    elseif name == "HotListSize"  and type( value )  == "number" then
      if value >= 10 and value <= 500 then
        ldtMap[M_HotListMax] = value;
      end
    elseif name == "HotListTransfer" and type( value ) == "number" then
      if value >= 2 and value <= ( ldtMap[M_HotListMax] - 2 ) then
        argListMap.HotListTransfer = value;
      end
    elseif name == "ByteEntrySize" and type( value ) == "number" then
      if value > 0 and value <= 4000 then
        ldtMap[M_LdrByteEntrySize] = value;
      end
    end
  end -- for each argument
      
  -- Do we need to reassign map to list?  We should not need this.
  -- ldtCtrl[2] = ldtMap;

  GP=E and trace("[EXIT]:<%s:%s>:LsoList after Init(%s)",
    MOD,meth,tostring(ldtCtrl));
  return ldtCtrl;
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
-- ldrSummary( ldrRec )
-- ======================================================================
-- Print out interesting stats about this LDR Chunk Record
-- ======================================================================
local function  ldrSummary( ldrRec ) 
  if( ldrRec  == nil ) then
    return "NULL Data Chunk (LDR) RECORD";
  end;
  if( ldrRec[LDR_CTRL_BIN]  == nil ) then
    return "NULL LDR CTRL BIN";
  end;
  if( ldrRec[SUBREC_PROP_BIN]  == nil ) then
    return "NULL LDR PROPERTY BIN";
  end;

  local resultMap = map();
  local ldrMap = ldrRec[LDR_CTRL_BIN];
  local ldrPropMap = ldrRec[SUBREC_PROP_BIN];

  resultMap.SelfDigest   = ldrPropMap[PM_SelfDigest];
  resultMap.ParentDigest   = ldrPropMap[PM_ParentDigest];

  resultMap.WarmList = ldrRec[LDR_LIST_BIN];
  resultMap.ListSize = list.size( resultMap.WarmList );

  return tostring( resultMap );
end -- ldrSummary()

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
--   (*) ldtCtrl:
--   (*) entryList:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readEntryList( resultList, ldtCtrl, entryList, count,
    func, fargs, all)

  local meth = "readEntryList()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
      MOD,meth,tostring(count), tostring(func), tostring(fargs),tostring(all));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  local doUnTransform = false; 
  if( ldtMap[M_UnTransform] ~= nil ) then
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
      readValue = functionTable[ldtMap[M_UnTransform]]( entryList[i] );
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

    if( resultValue ~= nil ) then
      list.append( resultList, readValue );
    end

--    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
--      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == false then
      GP=E and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s)",
        MOD, meth, numRead, summarizeList( resultList ));
      return numRead;
    end
  end -- for each entry in the list

  GP=E and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
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
--   (*) ldtCtrl
--   (*) LDR Chunk Page:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readByteArray( resultList, ldtCtrl, ldrChunk, count,
                              func, fargs, all)
  local meth = "readByteArray()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
    MOD,meth,tostring(count), tostring(func), tostring(fargs), tostring(all));
            
  local ldtMap = ldtCtrl[2];

  local doUnTransform = false;
  if( ldtMap[M_UnTransform] ~= nil ) then
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
  local entrySize = ldtMap[M_LdrByteEntrySize]; -- Entry Size in Bytes
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
      readValue = functionTable[ldtMap[M_UnTransform]]( byteValue );
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
      GP=E and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultList(%s)",
        MOD, meth, numRead, tostring( resultList ));
      return numRead;
    end
  end -- for each entry in the list (packed byte array)

  GP=E and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readByteArray()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Data Record (LDR) FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- LDR routines act specifically on the LDR "Data Chunk" records.

-- ======================================================================
-- ldrInsertList( ldrRec, ldtMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrRec: Hotest of the Warm Chunk Records
-- (*) ldtMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertList(ldrRec,ldtMap,listIndex,insertList )
  local meth = "ldrInsertList()";
  GP=E and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  GP=F and trace("[DEBUG]<%s:%s> LSO MAP(%s)", MOD, meth, tostring(ldtMap));

  local ldrMap = ldrRec[LDR_CTRL_BIN];
  local ldrValueList = ldrRec[LDR_LIST_BIN];
  local chunkIndexStart = list.size( ldrValueList ) + 1;
  local ldrByteArray = ldrRec[LDR_BNRY_BIN]; -- might be nil

  GP=F and trace("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSlotsAvailable = (ldtMap[M_LdrEntryCountMax] - chunkIndexStart) + 1;

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
    ldtMap[M_WarmTopFull] = true; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d)::SpaceAvail(%d):WTop FULL!!",
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
  GP=F and trace("[DEBUG]<%s:%s>ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, listIndex, chunkIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  ldrRec[LDR_CTRL_BIN] = ldrMap;
  ldrRec[LDR_LIST_BIN] = ldrValueList;

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()


-- ======================================================================
-- ldrInsertBytes( topWarmChunk, ldtMap, listIndex,  insertList )
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
-- (*) ldtMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertBytes( ldrChunkRec, ldtMap, listIndex, insertList )
  local meth = "ldrInsertBytes()";
  GP=E and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = ldtMap[M_LdrByteEntrySize];
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
  GP=F and trace("[DEBUG]<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount);

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local maxEntries = math.floor(ldtMap[M_LdrByteCountMax] / entrySize );
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
    ldtMap[M_WarmTopFull] = true; -- Remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d)::SpaceAvail(%d):WTop FULL!!",
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
    GP=F and trace("[DEBUG]<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));
  else
    GP=F and
    trace("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));

    -- The API for this call changed (July 2, 2013).  Now use "ensure"
    -- bytes.set_len(ldrChunkRec[LDR_BNRY_BIN], totalSpaceNeeded );
    bytes.ensure(ldrChunkRec[LDR_BNRY_BIN], totalSpaceNeeded, 1);

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

  GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) SpaceAvail(%d) ByteStart(%d)",
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

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()

-- ======================================================================
-- ldrInsert( topWarmChunk, ldtMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in SM_LIST mode or
-- SM_BINARY mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsert(ldrChunkRec,ldtMap,listIndex,insertList )
  local meth = "ldrInsert()";
  GP=E and trace("[ENTER]: <%s:%s> Index(%d) List(%s), ChunkSummary(%s)",
    MOD, meth, listIndex, tostring( insertList ),ldrSummary(ldrChunkRec));

  if ldtMap[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,ldtMap,listIndex,insertList );
  else
    return ldrInsertBytes(ldrChunkRec,ldtMap,listIndex,insertList );
  end
end -- ldrInsert()

-- ======================================================================
-- ldrChunkRead( ldrChunk, resultList, ldtCtrl, count, func, fargs, all );
-- ======================================================================
-- Read ALL, or up to 'count' items from this chunk, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the chunk in FIFO order.
-- Parms:
-- (*) ldrChunk: Record object for the warm or cold LSO Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LSO Control info
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to filter read items
-- (*) fargs: Function Argument list for inner UDF
-- Return: the NUMBER of items read from this chunk.
-- ======================================================================
local function ldrChunkRead( ldrChunk, resultList, ldtCtrl, count,
                             func, fargs, all )
  local meth = "ldrChunkRead()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d) All(%s)",
      MOD, meth, count, tostring(all));

  -- Extract the property map and lso control map from the lso bin list.
  -- local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local storeMode = ldtMap[M_StoreMode];

  -- If the page is SM_BINARY mode, then we're using the "Binary" Bin
  -- LDR_BNRY_BIN, otherwise we're using the "List" Bin LDR_LIST_BIN.
  local numRead = 0;
  if ldtMap[M_StoreMode] == SM_LIST then
    local chunkList = ldrChunk[LDR_LIST_BIN];
    numRead = readEntryList(resultList, ldtCtrl, chunkList, count,
                            func, fargs, all);
  else
    numRead = readByteArray(resultList, ldtCtrl, ldrChunk, count,
                            func, fargs, all);
  end

  GP=E and trace("[EXIT]: <%s:%s> NumberRead(%d) ResultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- ldrChunkRead()
-- ======================================================================

-- ======================================================================
-- digestListRead(topRec, resultList, ldtCtrl, Count, func, fargs, all)
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LSO Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == true, read all items, regardless of "count".
-- Return: Return the amount read from the Digest List.
-- ======================================================================
local function digestListRead(src, topRec, resultList, ldtCtrl, digestList,
                              count, func, fargs, all)
  local meth = "digestListRead()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d) all(%s)",
    MOD, meth, count, tostring(all) );

  GP=F and trace("[DEBUG]: <%s:%s> Count(%d) DigList(%s) ResList(%s)",
    MOD, meth, count, tostring( digestList), tostring( resultList ));

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

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
    ldrChunkRead( ldrChunk, resultList, ldtCtrl, remaining, func, fargs, all );
    totalAmountRead = totalAmountRead + chunkItemsRead;

    GP=F and
    trace("[DEBUG]:<%s:%s>:after ChunkRead:NumRead(%d)DirIndex(%d)ResList(%s)", 
      MOD, meth, chunkItemsRead, dirIndex, tostring( resultList ));
    -- Early exit ONLY when ALL flag is not set.
    if( all == false and
      ( chunkItemsRead >= remaining or totalAmountRead >= count ) )
    then
      GP=E and trace("[Early EXIT]:<%s:%s>totalAmountRead(%d) ResultList(%s) ",
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

  GP=E and trace("[EXIT]: <%s:%s> totalAmountRead(%d) ResultListSummary(%s) ",
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
-- hotListRead( resultList, ldtCtrl, count, func, fargs );
-- ======================================================================
-- Parms:
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main Lso Control Structure
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: Boolean: when true, read ALL
-- Return 'count' items from the Hot List
local function hotListRead( resultList, ldtCtrl, count, func, fargs, all)
  local meth = "hotListRead()";
  GP=E and trace("[ENTER]:<%s:%s>Count(%d) All(%s)",
      MOD, meth, count, tostring( all ) );

  local ldtMap = ldtCtrl[2];
  local hotList = ldtMap[M_HotEntryList];

  local numRead =
    readEntryList(resultList, ldtCtrl, hotList, count, func, fargs, all);

  GP=E and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    MOD, meth, summarizeList(resultList) );
  return resultList;
end -- hotListRead()
-- ======================================================================

-- ======================================================================
-- extractHotListTransferList( ldtMap )
-- ======================================================================
-- Extract the oldest N elements (as defined in ldtMap) and create a
-- list that we return.  Also, reset the HotList to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTES:
-- (1) We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB_RECORD operations have succeeded.
-- (2) We don't need to use ldtCtrl as a parameter -- ldtMap is ok here.
-- ======================================================================
local function extractHotListTransferList( ldtMap )
  local meth = "extractHotListTransferList()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = ldtMap[M_HotListTransfer];
  local oldHotEntryList = ldtMap[M_HotEntryList];
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
  ldtMap[M_HotEntryList] = newHotEntryList;
  oldHotEntryList = nil;
  local helic = ldtMap[M_HotEntryListItemCount];
  ldtMap[M_HotEntryListItemCount] = helic - transAmount;

  GP=E and trace("[EXIT]: <%s:%s> ResultList(%s)",
    MOD, meth, summarizeList(resultList));
  return resultList;
end -- extractHotListTransferList()


-- ======================================================================
-- hotListHasRoom( ldtMap, insertValue )
-- ======================================================================
-- Return true if there's room, otherwise return false.
-- (*) ldtMap: the map for the LSO Bin
-- (*) insertValue: the new value to be pushed on the stack
-- NOTE: This is in its own function because it is possible that we will
-- want to add more sophistication in the future.
local function hotListHasRoom( ldtMap, insertValue )
  local meth = "hotListHasRoom()";
  GP=E and trace("[ENTER]: <%s:%s> : ", MOD, meth );
  local result = true;  -- This is the usual case

  local hotListLimit = ldtMap[M_HotListMax];
  local hotList = ldtMap[M_HotEntryList];
  if list.size( hotList ) >= hotListLimit then
    return false;
  end

  GP=E and trace("[EXIT]: <%s:%s> Result(%s) : ", MOD, meth, tostring(result));
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
-- (*) ldtCtrl: the control structure for the LSO Bin
-- (*) newStorageValue: the new value to be pushed on the stack
local function hotListInsert( ldtCtrl, newStorageValue  )
  local meth = "hotListInsert()";
  GP=E and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    MOD, meth, tostring(newStorageValue) );

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Update the hot list with a new element (and update the map)
  local hotList = ldtMap[M_HotEntryList];
  GP=F and trace("[HEY!!]<%s:%s> Appending to Hot List(%s)", 
    MOD, meth,tostring(hotList));
  -- list.append( ldtMap[M_HotEntryList], newStorageValue );
  list.append( hotList, newStorageValue );
  ldtMap[M_HotEntryList] = hotList;
  --
  -- Update the count (overall count and hot list count)
  local itemCount = propMap[PM_ItemCount];
  propMap[PM_ItemCount] = (itemCount + 1);

  local hotCount = ldtMap[M_HotEntryListItemCount];
  ldtMap[M_HotEntryListItemCount] = (hotCount + 1);

  GP=E and trace("[EXIT]: <%s:%s> : LSO List Result(%s)",
    MOD, meth, tostring( ldtCtrl ) );

  return 0;  -- all is well
end -- hotListInsert()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||         WARM LIST FUNCTIONS         ||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- ======================================================================
-- warmListChunkCreate()
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) ldtCtrl: The main structure of the LSO Bin.
-- ======================================================================
-- Create and initialize a new LDR "chunk", load the new digest for that
-- new chunk into the ldtMap (the warm dir list), and return it.
local function   warmListChunkCreate( src, topRec, ldtCtrl )
  local meth = "warmListChunkCreate()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  --
  -- Remember to add the newLdrChunkRecord to the SRC
  local newLdrChunkRecord = aerospike:create_subrec( topRec );
  local ldrPropMap = map();
  local ldrMap = map();
  local newChunkDigest = record.digest( newLdrChunkRecord );
  local lsoPropMap = ldtCtrl[1];
  local ldtMap     = ldtCtrl[2];
  local binName    = lsoPropMap[PM_BinName];

  initializeLdrMap(src,topRec,newLdrChunkRecord,ldrPropMap,ldrMap,ldtCtrl );

  -- Now that it's initialized, add the SUBREC to the SRC.
  addSubrecToContext( src, newLdrChunkRecord );

  -- Assign Prop, Control info and List info to the LDR bins
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;
  newLdrChunkRecord[LDR_CTRL_BIN] = ldrMap;
  newLdrChunkRecord[LDR_LIST_BIN] = list();

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrMap) );

  aerospike:update_subrec( newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDigestList
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
  GP=F and trace("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(ldtMap[M_WarmDigestList]));

  list.append( ldtMap[M_WarmDigestList], newChunkDigest );

  GP=F and trace("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LsoMap(%s)CH(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(ldtMap),
    tostring( ldtMap[M_ColdDirListHead] ));
   
  -- Increment the Warm Count
  local warmChunkCount = ldtMap[M_WarmListDigestCount];
  ldtMap[M_WarmListDigestCount] = (warmChunkCount + 1);

  -- NOTE: This may not be needed -- we may wish to update the topRec ONLY
  -- after all of the underlying SUB-REC  operations have been done.
  -- Update the top (LSO) record with the newly updated ldtMap;
  topRec[ binName ] = ldtMap;
  record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time

  GP=E and trace("[EXIT]: <%s:%s> Return(%s) ",
    MOD, meth, ldrSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  warmListChunkCreate()

-- ======================================================================
-- extractWarmListTransferList( ldtCtrl );
-- ======================================================================
-- Extract the oldest N digests from the WarmList (as defined in ldtMap)
-- and create a list that we return.  Also, reset the WarmList to exclude
-- these elements.  -- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTE: We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB-REC  operations have succeeded.
-- ======================================================================
local function extractWarmListTransferList( ldtCtrl )
  local meth = "extractWarmListTransferList()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Extract the main property map and lso control map from the ldtCtrl
  local lsoPropMap = ldtCtrl[1];
  local ldtMap     = ldtCtrl[2];

  -- Get the first N (transfer amount) list elements
  local transAmount = ldtMap[M_WarmListTransfer];
  local oldWarmDigestList = ldtMap[M_WarmDigestList];
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
  ldtMap[M_WarmDigestList] = newWarmDigestList;
  oldWarmDigestList = nil;
  ldtMap[M_WarmListDigestCount] = ldtMap[M_WarmListDigestCount] - transAmount;

  GP=E and trace("[EXIT]: <%s:%s> ResultList(%s) LsoMap(%s)",
      MOD, meth, summarizeList(resultList), tostring(ldtMap));

  return resultList;
end -- extractWarmListTransferList()

  
-- ======================================================================
-- warmListHasRoom( ldtMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) ldtMap: the map for the LSO Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmListHasRoom( ldtMap )
  local meth = "warmListHasRoom()";
  local decision = 1; -- Start Optimistic (most times answer will be YES)
  GP=E and trace("[ENTER]: <%s:%s> Bin Map(%s)", 
    MOD, meth, tostring( ldtMap ));

  if ldtMap[M_WarmListDigestCount] >= ldtMap[M_WarmListMax] then
    decision = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> Decision(%d)", MOD, meth, decision );
  return decision;
end -- warmListHasRoom()

-- ======================================================================
-- warmListRead(topRec, resultList, ldtCtrl, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: Pass the Warm list on to "digestListRead()" and let it do
-- all of the work.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Warm Dir List.
-- ======================================================================
local function warmListRead(src, topRec, resultList, ldtCtrl, count, func,
    fargs, all)

  local ldtMap  = ldtCtrl[2];
  local digestList = ldtMap[M_WarmDigestList];

  return digestListRead(src, topRec, resultList, ldtCtrl,
                          digestList, count, func, fargs, all);
end -- warmListRead()

-- ======================================================================
-- warmListGetTop()
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtMap: the LSO control Map (ldtCtrl not needed here)
-- ======================================================================
local function warmListGetTop( src, topRec, ldtMap )
  local meth = "warmListGetTop()";
  GP=E and trace("[ENTER]: <%s:%s> ldtMap(%s)", MOD, meth, tostring( ldtMap ));

  local warmDigestList = ldtMap[M_WarmDigestList];
  local stringDigest = tostring( warmDigestList[ list.size(warmDigestList) ]);

  GP=F and trace("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)", 
      MOD, meth, stringDigest, list.size( warmDigestList ));

  local topWarmChunk = aerospike:open_subrec( topRec, stringDigest );

  GP=E and trace("[EXIT]: <%s:%s> result(%s) ",
    MOD, meth, ldrSummary( topWarmChunk ) );
  return topWarmChunk;
end -- warmListGetTop()

-- ======================================================================
-- warmListInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the warm
-- dir list -- a directory of warm Lso Data Records that will contain 
-- the data entries.
-- A New Feature to insert is the "StoreLimit" aspect.  If we are over the
-- storage limit, then before we insert into the warm list, we're going to
-- release some old storage.  This storage may be in the Warm List or in
-- the Cold List (or both), however, we only care about WarmList storage
-- BEFORE the warmlist insert. 
-- Notice that we're basically dealing in item counts, NOT in total storage
-- bytes.  That (total byte storage) is future work.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtCtrl: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function warmListInsert( src, topRec, ldtCtrl, entryList )
  local meth = "warmListInsert()";
  local rc = 0;

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local binName = propMap[PM_BinName];

  GP=E and trace("[ENTER]: <%s:%s> WDL(%s)",
    MOD, meth, tostring(ldtMap[M_WarmDigestList]));

  GP=F and trace("[DEBUG]:<%s:%s> LSO LIST(%s)", MOD, meth, tostring(ldtCtrl));

  local warmDigestList = ldtMap[M_WarmDigestList];
  local topWarmChunk;

  -- With regard to the Ldt Data Record (LDR) Pages, whether we create a new
  -- LDR or open an existing LDR, we save the current count and close the
  -- LDR page.
  -- Note that the last write may have filled up the warmTopChunk, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
  if list.size( warmDigestList ) == 0 or ldtMap[M_WarmTopFull] == true then
    GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
    topWarmChunk = warmListChunkCreate(src, topRec, ldtCtrl ); -- create new
    ldtMap[M_WarmTopFull] = false; -- reset for next time.
  else
    GP=F and trace("[DEBUG]: <%s:%s> Calling Get TOP ", MOD, meth );
    topWarmChunk = warmListGetTop( src, topRec, ldtMap ); -- open existing
  end
  GP=F and trace("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) ", 
    MOD, meth, tostring( ldtMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalEntryCount = list.size( entryList );
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  local countWritten = ldrInsert( topWarmChunk, ldtMap, 1, entryList );
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert(1)", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end
  local itemsLeft = totalEntryCount - countWritten;
  if itemsLeft > 0 then
    aerospike:update_subrec( topWarmChunk );

    aerospike:close_subrec( topWarmChunk );

    GP=F and trace("[DEBUG]:<%s:%s>Calling Chunk Create: AGAIN!!", MOD, meth );
    topWarmChunk = warmListChunkCreate( src, topRec, ldtMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot list.
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)",
    MOD, meth, tostring( entryList ), countWritten + 1);
    countWritten =
        ldrInsert( topWarmChunk, ldtMap, countWritten+1, entryList );
    if( countWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert(2)", MOD, meth);
      error( ldte.ERR_INTERNAL );
    end
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) ",
        MOD, meth, countWritten, itemsLeft );
      error( ldte.ERR_INTERNAL );
    end
  end

  -- NOTE: We do NOT have to update the WarmDigest Count here; that is done
  -- in the warmListChunkCreate() call.

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( ldtMap ));
  topRec[binName] = ldtMap;
  record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    MOD, meth, ldrSummary( topWarmChunk ));

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


-- ======================================================================
-- releaseStorage():: @RAJ @TOBY TODO: Change inside to crec_release() call
-- ======================================================================
-- Release the storage in this digest list.  Either iterate thru the
-- list and release it immediately (if that's the only option), or
-- deliver the digestList to a component that can schedule the digest
-- to be cleaned up later.
-- ======================================================================
local function releaseStorage( topRec, ldtCtrl, digestList )
  local meth = "releaseStorage()";
  local rc = 0;
  GP=E and trace("[ENTER]:<%s:%s> lsoSummary(%s) digestList(%s)",
    MOD, meth, lsoSummaryString( ldtCtrl ), tostring(digestList));

    info("LSTACK Subrecord Eviction: Subrec List(%s)",tostring(digestList));

    local subrec;
    local digestString;
    local propMap = ldtCtrl[1];
    local ldtMap  = ldtCtrl[2];
    local binName = propMap[PM_BinName];

    if( digestList == nil or list.size( digestList ) == 0 ) then
      warn("[INTERNAL ERROR]<%s:%s> DigestList is nil or empty", MOD, meth );
    else
      local listSize = list.size( digestList );
      for i = 1, listSize, 1 do
        digestString = tostring( digestList[i] );
        local subrec = aerospike:open_subrec( topRec, digestString );
        rc = aerospike:remove_subrec( subrec );
        if( rc == nil or rc == 0 ) then
          GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
        else
          warn("[SUB DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, binName);
          error( ldte.ERR_SUBREC_DELETE );
        end
      end
    end

  GP=E and trace("[EXIT]: <%s:%s> ", MOD, meth );
end -- releaseStorage()

-- ======================================================================
-- Update the ColdDir Page pointers -- used on initial create
-- and subsequent ColdDir page creates.  Each Cold Dir Page has a
-- Previous and Next pointer (in the form of a digest).
-- Parms:
-- (*) coldDirRec:
-- (*) prevDigest:  Set PrevPage ptr, if not nil
-- (*) nextDigest:  Set NextPage ptr, if not nil
-- ======================================================================
local function setPagePointers( coldDirRec, prevDigest, nextDigest )
  local meth = "setLeafPagePointers()";
  GP=E and trace("[ENTER]<%s:%s> prev(%s) next(%s)",
    MOD, meth, tostring(prevDigest), tostring(nextDigest) );
  leafMap = leafRec[LSR_CTRL_BIN];
  if( prevDigest ~= nil ) then
    leafMap[LF_PrevPage] = prevDigest;
  end
  if( prevDigest ~= nil ) then
    leafMap[LF_NextPage] = nextDigest;
  end
  leafRec[LSR_CTRL_BIN] = leafMap;
  aerospike:update_subrec( leafRec );

  GP=E and trace("[EXIT]<%s:%s> ", MOD, meth );
end -- setPagePointers()

-- ======================================================================
-- coldDirHeadCreate()
-- ======================================================================
-- Set up a new Head Directory page for the cold list.  The Cold List Dir
-- pages each hold a list of digests to data pages.  Note that
-- the data pages (LDR pages) are already built from the warm list, so
-- the cold list just holds those LDR digests after the record agest out
-- of the warm list. 
--
-- New for the summer of 2013::We're going to allow data to gracefully age
-- out by limiting the number of active Cold Directory Pages that we'll have
-- in an LSTACK at one time. So, if the limit is set to "N", then we'll
-- have (N-1) FULL directory pages, and one directory page that is being
-- filled up.  We check ONLY when it's time to create a new directory head,
-- so that is on the order of once every 10,000 inserts (or so).
--
-- Parms:
-- (*) src: Subrec Context
-- (*) topRec: the top record -- needed when we create a new dir and LDR
-- (*) ldtCtrl: the control map of the top record
-- (*) Space Estimate of the number of items needed
-- Return:
-- Success: NewColdHead Sub-Record Pointer
-- Error:   Nil
-- ======================================================================
local function coldDirHeadCreate( src, topRec, ldtCtrl, spaceEstimate )
  local meth = "coldDirHeadCreate()";
  GP=E and trace("[ENTER]<%s:%s>LSO(%s)",MOD,meth,lsoSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local binName = propMap[PM_BinName];
  local ldrDeleteList; -- List of LDR subrecs to be removed (eviction)
  local dirDeleteList; -- List of Cold Directory Subrecs to be removed.
  local ldrItemCount = ldtMap[M_LdrEntryCountMax];
  local subrecDeleteCount; -- ALL subrecs (LDRs and Cold Dirs)
  local coldDirMap;
  local coldDirList;
  local coldDirRec;
  local returnColdHead; -- this is what we return
  local coldDirDigest;
  local coldDirDigestString;
  local createNewHead = true;
  local itemsDeleted = 0;
  local subrecsDeleted = 0;

  -- This is new code to deal with the expiration/eviction of old data.
  -- Usually, it will be data in the Cold List.  We will release cold list
  -- data when we create a new ColdDirectoryHead.  That's the best time
  -- to assess what's happening in the cold list. (July 2013: tjl)
  --
  -- In the unlikely event that the user has specified that they want ONE
  -- (and only one) Cold Dir Record, that means that we shouldn't actually
  -- create a NEW Cold Dir Record.  We should just free up the digest
  -- list (release the sub rec storage) and return the existing cold dir
  -- head -- just in a freshened state.
  local coldDirRecCount = ldtMap[M_ColdDirRecCount];
  local coldDirRecMax = ldtMap[M_ColdDirRecMax];
  GP=F and trace("[DEBUG]<%s:%s>coldDirRecCount(%s) coldDirRecMax(%s)",
    MOD, meth, tostring(coldDirRecCount), tostring(coldDirRecMax));
  if( coldDirRecMax == 1 and coldDirRecCount == 1 ) then
    GP=F and trace("[DEBUG]<%s:%s>Special Case ONE Dir", MOD, meth );
    -- We have the weird special case. We will NOT delete this Cold Dir Head
    -- and Create a new one.  Instead, we will just clean out
    -- the Digest List enough so that we have room for "newItemCount".
    -- We expect that in most configurations, the transfer list coming in
    -- will be roughly half of the array size.  We don't expect to see
    -- a "newCount" that is greater than the Cold Dir Limit.
    -- ALSO -- do NOT drop into the code below that Creates a new head.
    createNewHead = false;
    coldDirDigest = ldtMap[M_ColdDirListHead];
    coldDirDigestString = tostring( coldDirDigest );
    coldDirRec = openSubrec( src, topRec, coldDirDigestString );
    if( coldDirRec == nil ) then
      warn("[INTERNAL ERROR]<%s:%s> Can't open Cold Head(%s)", MOD, meth,
        coldDirDigestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
    coldDirList = coldDirRec[COLD_DIR_LIST_BIN];
    if( spaceEstimate >= ldtMap[M_ColdListMax] ) then
      -- Just clear out the whole thing.
      ldrDeleteList = coldDirList; -- Pass this on to "release storage"
      coldDirRec[COLD_DIR_LIST_BIN] = list(); -- reset the list.
    else
      -- Take gets the [1..N] elements.
      -- Drop gets the [(N+1)..end] elements (it drops [1..N] elements)
      ldrDeleteList = list.take( coldDirList, spaceEstimate );
      local saveList = list.drop( coldDirList, spaceEstimate );
      coldDirRec[COLD_DIR_LIST_BIN] = saveList;
    end

    -- Gather up some statistics:
    -- Track the sub-record count (LDRs and Cold Dirs).  Notice that the
    -- Cold Dir here stays, so we have only LDRs.
    subrecsDeleted = list.size( ldrDeleteList );
    -- Track the items -- assume that all of the SUBRECS were full.
    itemsDeleted = subrecsDeleted * ldrItemCount;

    -- Save the changes to the Cold Head
    updateSubrec( src, coldDirRec, coldDirDigest );
    returnColdHead = coldDirRec;

  elseif( coldDirRecCount >= coldDirRecMax ) then
    GP=F and trace("[DEBUG]<%s:%s>Release Cold Dirs: Cnt(%d) Max(%d)",
    MOD, meth, coldDirRecCount, coldDirRecMax );
    -- Release as many cold dirs as we are OVER the max.  Release
    -- them in reverse order, starting with the tail.  We put all of the
    -- LDR subrec digests in the delete list, followed by the ColdDir 
    -- subrec.
    local coldDirCount = (coldDirRecCount + 1) - coldDirRecMax;
    local tailDigest = ldtMap[M_ColdDirListTail];
    local tailDigestString = tostring( tailDigest );
    GP=F and trace("[DEBUG]<%s:%s>Cur Cold Tail(%s)", MOD, meth,
      tostring( tailDigestString ));
    ldrDeleteList = list();
    dirDeleteList = list();
    while( coldDirCount > 0 ) do
      if( tailDigestString == nil or tailDigestString == 0 ) then
        -- Something is wrong -- don't continue.
        warn("[INTERNAL ERROR]<%s:%s> Tail is broken", MOD, meth );
        break;
      else
        -- Open the Cold Dir Record, add the digest list to the delete
        -- list and move on to the next Cold Dir Record.
        -- Note that we track the LDRs and the DIRs separately.
        -- Also note the two different types of LIST APPEND.
        coldDirRec = openSubrec( src, topRec, tailDigestString );
        -- Append a digest LIST to the LDR delete list
        listAppend( ldrDeleteList, coldDirRec[COLD_DIR_LIST_BIN] );
        -- Append a cold Dir Digest to the DirDelete list
        list.append( dirDeleteList, tailDigest ); 

        -- Move back one to the previous ColdDir Rec.  Make it the
        -- NEW TAIL.
        coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
        tailDigest = coldDirMap[CDM_PrevDirRec];
        GP=F and trace("[DEBUG]<%s:%s> Cur Tail(%s) Next Cold Dir Tail(%s)",
          MOD, meth, tailDigestString, tostring(tailDigest) );
        tailDigestString = tostring(tailDigest);
        
        -- It is best to adjust the new tail now, even though in some
        -- cases we might remove this cold dir rec as well.
        coldDirRec = openSubrec( src, topRec, tailDigestString );
        coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
        coldDirMap[CDM_NextDirRec] = 0; -- this is now the tail

        -- If we go around again -- we'll need this.
        tailDigestString = record.digest( coldDirRec );
      end -- else tail digest ok
      coldDirCount = coldDirCount - 1; -- get ready for next iteration
    end -- while; count down Cold Dir Recs

    -- Update the LAST Cold Dir that we were in.  It's the new tail
    updateSubrec( src, coldDirRec, coldDirDigest );

    -- Gather up some statistics:
    -- Track the sub-record counts (LDRs and Cold Dirs). 
    -- Track the items -- assume that all of the SUBRECS were full.
    itemsDeleted = list.size(ldrDeleteList) * ldrItemCount;
    subrecsDeleted = list.size(ldrDeleteList) + list.size(dirDeleteList);

    -- releaseStorage( topRec, ldtCtrl, deleteList );
  end -- end -- cases for when we remove OLD storage

  -- If we did some deletes -- clean that all up now.
  -- Update the various statistics (item and subrec counts)
  if( itemsDeleted > 0 or subrecsDeleted > 0 ) then
    local subrecCount = propMap[PM_SubRecCount];
    propMap[PM_SubRecCount] = subrecCount - subrecsDeleted;

    local itemCount = propMap[PM_ItemCount];
    propMap[PM_ItemCount] = itemCount - itemsDeleted;


    -- Now release any freed subrecs.
    releaseStorage( topRec, ldtCtrl, ldrDeleteList );
    releaseStorage( topRec, ldtCtrl, dirDeleteList );
  end

  -- Now -- whether or not we removed some old storage above, NOW are are
  -- going to add a new Cold Directory HEAD.
  if( createNewHead == true ) then
    GP=F and trace("[DEBUG]<%s:%s>Regular Cold Head Case", MOD, meth );

    -- Create the Cold Head Record, initialize the bins: Ctrl, List
    -- Also -- now that we have a DOUBLY linked list, get the NEXT Cold Dir,
    -- if present, and have it point BACK to this new one.
    --
    -- Note: All Field Names start with UPPER CASE.
    -- Remember to add the newColdHeadRec to the SRC.
    local newColdHeadRec = aerospike:create_subrec( topRec );
    local newColdHeadMap     = map();
    local newColdHeadPropMap = map();
    initializeColdDirMap(topRec, newColdHeadRec, newColdHeadPropMap,
                         newColdHeadMap, ldtCtrl);

    -- Now that it's initialized, add the newColdHeadRec to the SRC.
    addSubrecToContext( src, newColdHeadRec );
                           
    -- Update our global counts ==> One more Cold Dir Record.
    ldtMap[M_ColdDirRecCount] = coldDirRecCount + 1;

    -- Plug this directory into the (now doubly linked) chain of Cold Dir
    -- Records (starting at HEAD).
    local oldColdHeadDigest = ldtMap[M_ColdDirListHead];
    local newColdHeadDigest = newColdHeadPropMap[PM_SelfDigest];

    newColdHeadMap[CDM_NextDirRec] = oldColdHeadDigest;
    newColdHeadMap[CDM_PrevDirRec] = 0; -- Nothing ahead of this one, yet.
    ldtMap[M_ColdDirListHead] = newColdHeadPropMap[PM_SelfDigest];

    GP=F and trace("[DEBUG]<%s:%s> New ColdHead = (%s) Cold Next = (%s)",
      MOD, meth, tostring(newColdHeadDigest),tostring(oldColdHeadDigest));

    -- Get the NEXT Cold Dir (the OLD Head) if there is one, and set it's
    -- PREV pointer to THIS NEW HEAD.  This is the one downfall for having a
    -- double linked list, but since we now need to traverse the list in
    -- both directions, it's a necessary evil.
    if( oldColdHeadDigest == nil or oldColdHeadDigest == 0 ) then
      -- There is no Next Cold Dir, so we're done.
      GP=F and trace("[DEBUG]<%s:%s> No Next CDir (assign ZERO)",MOD, meth );
    else
      -- Regular situation:  Go open the old ColdDirRec and update it.
      local oldColdHeadDigestString = tostring(oldColdHeadDigest);
      local oldColdHeadRec = openSubrec(src,topRec,oldColdHeadDigestString);
      if( oldColdHeadRec == nil ) then
        warn("[ERROR]<%s:%s> oldColdHead NIL from openSubrec: digest(%s)",
          MOD, meth, oldColdHeadDigestString );
        error( ldte.ERR_SUBREC_OPEN );
      end
      local oldColdHeadMap = oldColdHeadRec[COLD_DIR_CTRL_BIN];
      oldColdHeadMap[CDM_PrevDirRec] = newColdHeadDigest;

      updateSubrec( src, oldColdHeadRec, oldColdHeadDigest );
      -- aerospike:update_subrec( oldColdHeadRec );
    end

    GP=F and trace("[REVIEW]: <%s:%s> LSOMAP = (%s) COLD DIR PROP MAP = (%s)",
      MOD, meth, tostring(ldtMap), tostring(newColdHeadPropMap));

    -- Save our updates in the records
    newColdHeadRec[COLD_DIR_LIST_BIN] = list(); -- allocate a new digest list
    newColdHeadRec[COLD_DIR_CTRL_BIN] = newColdHeadMap;
    newColdHeadRec[SUBREC_PROP_BIN] =   newColdHeadPropMap;

    aerospike:update_subrec( newColdHeadRec );

    -- NOTE: We don't want to update the TOP RECORD until we know that
    -- the  underlying children record operations are complete.
    -- However, we can update topRec here, since that won't get written back
    -- to storage until there's an explicit update_subrec() call.
    topRec[ binName ] = ldtMap;
    record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time
    returnColdHead = newColdHeadRec;
  end -- if we should create a new Cold HEAD

  GP=E and trace("[EXIT]: <%s:%s> New Cold Head Record(%s) ",
    MOD, meth, coldDirRecSummary( returnColdHead ));
  return returnColdHead;
end --  coldDirHeadCreate()()

-- ======================================================================
-- coldDirRecInsert(ldtCtrl, coldHeadRec,digestListIndex,digestList)
-- ======================================================================
-- Insert as much as we can of "digestList", which is a list of digests
-- to LDRs, into a -- Cold Directory Page.  Return num written.
-- It is the caller's job to allocate a NEW Dir Rec page if not all of
-- digestList( digestListIndex to end) fits.
-- Parms:
-- (*) ldtCtrl: the main control structure
-- (*) coldHeadRec: The Cold List Directory Record
-- (*) digestListIndex: The starting Read position in the list
-- (*) digestList: the list of digests to be inserted
-- Return: Number of digests written, -1 for error.
-- ======================================================================
local function coldDirRecInsert(ldtCtrl,coldHeadRec,digestListIndex,digestList)
  local meth = "coldDirRecInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]:<%s:%s> ColdHead(%s) ColdDigestList(%s)",
      MOD, meth, coldDirRecSummary(coldHeadRec), tostring( digestList ));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldDirList = coldHeadRec[COLD_DIR_LIST_BIN];
  local coldDirMax = ldtMap[M_ColdListMax];

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
    ldtMap[M_ColdTopFull] = true; -- Now, remember to reset on next update.
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

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) Digest List(%s) map(%s)",
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
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtCtrl: the control map of the top record
-- (*) digestList: the list of digests to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function coldListInsert( src, topRec, ldtCtrl, digestList )
  local meth = "coldListInsert()";
  local rc = 0;

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local binName = propMap[PM_BinName];

  GP=E and trace("[ENTER]<%s:%s>SRC(%s) LSO Summary(%s) DigestList(%s)", MOD,
    meth, tostring(src), lsoSummaryString(ldtCtrl), tostring( digestList ));

  GP=F and trace("[DEBUG 0]:Map:WDL(%s)", tostring(ldtMap[M_WarmDigestList]));

  -- The very first thing we must check is to see if we are ALLOWED to have
  -- a cold list.  If M_ColdDirRecMax is ZERO, then that means we are
  -- not having a cold list -- so the warmListTransfer data is effectively
  -- being deleted.  If that's the case, then we pass those digests to
  -- the "release storage" method and return.
  if( ldtMap[M_ColdDirRecMax] == 0 ) then
    rc = releaseStorage( topRec, ldtCtrl, digestList );
    GP=E and trace("[Early EXIT]: <%s:%s> Release Storage Status(%s) RC(%d)",
      MOD,meth, tostring(status), rc );
    return rc;
  end

  -- Ok, we WILL do cold storage, so we have to check the status.
  -- If we don't have a cold list, then we have to build one.  Also, if
  -- the current cold Head is completely full, then we also need to add
  -- a new one.  And, if we ADD one, then we have to check to see if we
  -- need to delete the oldest one (or more than one).
  local stringDigest;
  local coldHeadRec;
  local transferAmount = list.size( digestList );

  local coldHeadDigest = ldtMap[M_ColdDirListHead];
  GP=F and trace("[DEBUG]<%s:%s>Cold List Head Digest(%s), ColdFullorNew(%s)",
      MOD, meth, tostring( coldHeadDigest), tostring(ldtMap[M_ColdTopFull]));

  if coldHeadDigest == nil or
     coldHeadDigest == 0 or
     ldtMap[M_ColdTopFull] == true
  then
    -- Create a new Cold Directory Head and link it in the Dir Chain.
    GP=F and trace("[DEBUG]:<%s:%s>:Creating FIRST NEW COLD HEAD", MOD, meth );
    coldHeadRec = coldDirHeadCreate(src, topRec, ldtCtrl, transferAmount );
    coldHeadDigest = record.digest( coldHeadRec );
    stringDigest = tostring( coldHeadDigest );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Opening Existing COLD HEAD", MOD, meth );
    stringDigest = tostring( coldHeadDigest );
    coldHeadRec = aerospike:open_subrec( topRec, stringDigest );
  end

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldHeadList = coldHeadRec[COLD_DIR_LIST_BIN];

  GP=F and trace("[DEBUG]<%s:%s>Digest(%s) ColdHeadCtrl(%s) ColdHeadList(%s)",
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
      coldDirRecInsert(ldtCtrl, coldHeadRec, digestListIndex, digestList);
    if( digestsWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Cold Dir Insert", MOD, meth);
      error( ldte.ERR_INSERT );
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
      -- Note that coldDirHeadCreate() deals with the data Expiration and
      -- eviction for any data that is in cold storage.
      coldHeadRec = coldDirHeadCreate( topRec, ldtCtrl, digestsLeft );
    end
  end -- while digests left to write.
  
  -- Update the Cold List Digest Count (add to cold, subtract from warm)
  local coldDataRecCount = ldtMap[M_ColdDataRecCount];
  ldtMap[M_ColdDataRecCount] = coldDataRecCount + transferAmount;

  local warmListCount = ldtMap[M_WarmListDigestCount];
  ldtMap[M_WarmListDigestCount] = warmListCount - transferAmount;

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( ldtMap ));
  topRec[ binName ] = ldtMap;
  record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]: <%s:%s> New Cold Head Save: Summary(%s) ",
    MOD, meth, coldDirRecSummary( coldHeadRec ));
  local status = aerospike:update_subrec( coldHeadRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",
    MOD,meth, tostring(status));

  status = aerospike:close_subrec( coldHeadRec );
  GP=E and trace("[EXIT]: <%s:%s> SUB-REC  Close Status(%s) RC(%d)",
    MOD,meth, tostring(status), rc );

  -- Note: This is warm to cold transfer only.  So, no new data added here,
  -- and as a result, no new counts to upate (just warm/cold adjustments).

  return rc;
end -- coldListInsert


-- ======================================================================
-- coldListRead(topRec, resultList, ldtCtrl, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: March down the Cold List Directory Pages (a linked list of
-- directory pages -- that each point to Lso Data Record "chunks") and
-- read "count" data entries.  Use the same ReadDigestList method as the
-- warm list.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Cold Dir List.
-- ======================================================================
local function
coldListRead(src, topRec, resultList, ldtCtrl, count, func, fargs, all)
  local meth = "coldListRead()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d) All(%s) ldtMap(%s)",
      MOD, meth, count, tostring( all ), tostring( ldtMap ));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- If there is no Cold List, then return immediately -- nothing read.
  if(ldtMap[M_ColdDirListHead] == nil or ldtMap[M_ColdDirListHead] == 0) then
    GP=F and trace("[WARNING]: <%s:%s> LSO MAP COLD LIST Head is Nil/ZERO",
      MOD, meth, count, tostring( all ));
    return 0;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = ldtMap[M_ColdDirListHead];

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

    numRead = digestListRead(src, topRec, resultList, ldtCtrl, digestList,
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
    GP=F and trace("[DEBUG]:<%s:%s>:CountRemain(%d) NextDir(%s)PrevDir(%s)",
          MOD, meth, countRemaining, tostring(coldDirMap[CDM_NextDirRec]),
          tostring(coldDirMap[CDM_PrevDirRec]));

    if countRemaining <= 0 or coldDirMap[CDM_NextDirRec] == 0 then
        GP=E and trace("[EARLY EXIT]:<%s:%s>:Cold Read: (%d) Items",
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
      MOD, meth, tostring( ldtMap ), tostring( coldDirMap )); 

  GP=E and trace("[EXIT]:<%s:%s>totalAmountRead(%d) ResultListSummary(%s) ",
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
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: The top level user record (needed for create_subrec)
-- (*) ldtCtrl
-- Return: Success (0) or Failure (-1)
-- ======================================================================
local function warmListTransfer( src, topRec, ldtCtrl )
  local meth = "warmListTransfer()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>\n\n <> TRANSFER TO COLD LIST <> lso(%s)\n",
    MOD, meth, tostring(ldtCtrl) );

  -- if we haven't yet initialized the cold list, then set up the
  -- first Directory Head (a page of digests to data pages).  Note that
  -- the data pages are already built from the warm list, so all we're doing
  -- here is moving the reference (the digest value) from the warm list
  -- to the cold directory page.

  -- Build the list of items (digests) that we'll be moving from the warm
  -- list to the cold list. Use coldListInsert() to insert them.
  local transferList = extractWarmListTransferList( ldtCtrl );
  rc = coldListInsert( src, topRec, ldtCtrl, transferList );
  GP=E and trace("[EXIT]: <%s:%s> lso(%s) ", MOD, meth, tostring(ldtCtrl) );
  return rc;
end -- warmListTransfer()


-- ======================================================================
-- local function hotListTransfer( ldtCtrl, insertValue )
-- ======================================================================
-- The job of hotListTransfer() is to move part of the HotList, as
-- specified by HotListTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmDigestList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec
-- (*) ldtCtrl
-- ======================================================================
local function hotListTransfer( src, topRec, ldtCtrl )
  local meth = "hotListTransfer()";
  local rc = 0;
  GP=E and trace("[ENTER]: <%s:%s> LSO Summary(%s) ",
      MOD, meth, tostring( lsoSummary(ldtCtrl) ));
      --
  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmListHasRoom( ldtMap ) == 0 then
    warmListTransfer( src, topRec, ldtCtrl );
  end

  -- Do this the simple (more expensive) way for now:  Build a list of the
  -- items (data entries) that we're moving from the hot list to the warm dir,
  -- then call insertWarmDir() to find a place for it.
  local transferList = extractHotListTransferList( ldtMap );
  rc = warmListInsert( src, topRec, ldtCtrl, transferList );

  GP=E and trace("[EXIT]: <%s:%s> result(%d) LsoMap(%s) ",
    MOD, meth, rc, tostring( ldtMap ));
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
  GP=E and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
      MOD, meth, tostring(binName));

  if binName == nil  then
    warn("[ERROR EXIT]:<%s:%s> Null Bin Name", MOD, meth );
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( binName ) ~= "string"  then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Not a String", MOD, meth );
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( binName ) > 14 then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Too Long", MOD, meth );
    error( ldte.ERR_BIN_NAME_TOO_LONG );
  end
  GP=E and trace("[EXIT]:<%s:%s> Ok", MOD, meth );
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
  GP=E and trace("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
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
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end

    -- Control Bin Must Exist
    if( topRec[lsoBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSO BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(lsoBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    local ldtCtrl = topRec[lsoBinName]; -- The main ldtMap structure
    -- Extract the property map and lso control map from the lso bin list.
    local propMap = ldtCtrl[1];
    local ldtMap  = ldtCtrl[2];

    if propMap[PM_Magic] ~= MAGIC then
      GP=E and warn("[ERROR EXIT]:<%s:%s>LSO BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( lsoBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[lsoBinName] ~= nil then
      local ldtCtrl = topRec[lsoBinName]; -- The main ldtMap structure
      -- Extract the property map and lso control map from the lso bin list.
      local propMap = ldtCtrl[1];
      local ldtMap  = ldtCtrl[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=E and warn("[ERROR EXIT]:<%s:%s> LSO BIN(%s) Corrupted (no magic)2",
              MOD, meth, tostring( lsoBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
    end -- if worth checking
  end -- else for must exist
  GP=E and trace("[EXIT]:<%s:%s> Ok", MOD, meth );

end -- validateRecBinAndMap()

-- ========================================================================
-- buildSubRecList()
-- ========================================================================
-- Build the list of subrecs starting at location N.  ZERO means, get them
-- all (which is what lstack_delete() uses).
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtCtrl: The main LDT control structure
-- (3) position: We start building the list with the first subrec that
--     holds "position" (item count, not byte count).  If position is in
--     the HotList, then all Warm and Cold recs are included.
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- ========================================================================
local function buildSubRecList( topRec, ldtCtrl, position )
  local meth = "buildSubRecList()";

  GP=E and trace("[ENTER]: <%s:%s> position(%s) lsoSummary(%s)",
    MOD, meth, tostring(position), lsoSummaryString( ldtCtrl ));

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  info("\n\n [WARNING]<%s:%s> UNDER CONSTRUCTION !!!!!!!!!\n",MOD, meth);
  info("\n\n [WARNING]<%s:%s> UNDER CONSTRUCTION !!!!!!!!!\n",MOD, meth);

  -- If position puts us into or past the warmlist, make the adjustment
  -- here.  Otherwise, drop down into the FULL MONTY
  --
  if( position < 0 ) then
    warn("[ERROR]<%s:%s> BUILD SUBREC LIST ERROR: Bad Position(%d)",
      MOD, meth, position );
    error( ldte.ERR_INTERNAL );
  end

  -- Call buildSearchPath() to give us a searchPath object that shows us
  -- the storage we are going to release.  It will tell us what do to in
  -- each of the three types of storage: Hot, Warm and Cold, although, we
  -- care only about warm and cold in this case
  local searchPath = map();
  buildSearchPath( topRec, ldtCtrl, searchPath, position );

  -- Use the search path to show us where to start collecting digests in
  -- the WARM List.
  local wdList = ldtMap[M_WarmDigestList];
  local warmListSize = list.size(  wdList );

  -- If warmListStart is outside the size of the list, then that means we
  -- will just skip the for loop for the warm list.  Also, if the WarmPosition
  -- is ZERO, then we treat that as the same case.
  local resultList;
  local warmListStart = searchPath.WarmPosition;
  if( warmListStart == 0 or warmListStart > warmListSize ) then
    trace("[REC LIST]<%s:%s> Skipping over warm list: Size(%d) Pos(%d)",
      MOD, meth, warmListSize, warmListStart );
    resultList = list(); -- add only cold list items
  elseif( warmListStart == 1 ) then
    -- Take it all
    resultList = list.take( wdList, warmListSize );
  else
    -- Check this
    resultList = list.drop( wdList, warmListStart - 1 );
  end

  -- Now for the harder part.  We will still have open the cold list directory
  -- subrecords to know what is inside.  The searchPath is going to give us
  -- a digest position in the cold list.  We will open each Cold Directory
  -- Page until we get to the start position (which can be 1, or N)
  local count = 0;

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(ldtMap[M_ColdDirListHead] == nil or ldtMap[M_ColdDirListHead] == 0) then
    return resultList;
  end

  -- The challenge here is to collect the digests of all of the subrecords
  -- that are to be released.

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = ldtMap[M_ColdDirListHead];

  -- LEFT OFF HERE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  -- Note that we're not using this function in production (whew)
  info("\n\n LEFT OFF HERE<%s:%s>!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n",MOD, meth);
  info("\n\n LEFT OFF HERE<%s:%s>!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n",MOD, meth);

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
  end -- for each coldDirRecDigest

  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList
end -- buildResultList()


-- ========================================================================
-- buildSubRecListAll()
-- ========================================================================
-- Build the list of subrecs for the entire LDT.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the user-level record holding the LSO Bin
-- (*) ldtCtrl: The main LDT control structure
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- ========================================================================
function buildSubRecListAll( src, topRec, lsolist )
  local meth = "buildSubRecListAll()";

  GP=E and trace("[ENTER]: <%s:%s> LSO Summary(%s)",
    MOD, meth, lsoSummaryString( ldtCtrl ));

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Copy the warm list into the result list
  local wdList = ldtMap[M_WarmDigestList];
  local transAmount = list.size( wdList );
  local resultList = list.take( wdList, transAmount );

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(ldtMap[M_ColdDirListHead] == nil or ldtMap[M_ColdDirListHead] == 0) then
    return resultList;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = ldtMap[M_ColdDirListHead];

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

  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList

end -- buildSubRecListAll()

-- ======================================================================
-- createSearchPath()
-- ======================================================================
-- Create a searchPath object for LSTACK that provides the details in the
-- stack for the object position.
-- Different from LLIST search position, which shows the path from the
-- Tree root all the way down to the tree leaf (and all of the inner nodes
-- from the root to the leaf), the SearchPath for a stack shows the
-- relative location in either
-- (*) The Hot List::(simple position in the directory)
-- (*) The Warm List::(Digest position in the warm list, plus the position
--     in the LDR objectList)
-- (*) The Cold List::(Cold Dir digest, plus position in the cold Dir
--     Digest List, plus position in the LDR.
-- ======================================================================


-- ======================================================================
-- locatePosition()
-- ======================================================================
-- Create a Search Path Object that shows where "position" lies in the
-- stack object.  The possible places are:
-- (*) Hot List::  Entry List Position
-- (*) Warm List:: Digest List Position, Entry List Position
-- (*) Cold List:: Directory List Pos, Digest List Pos, Entry List Pos
-- We don't open up every subrec.  We assume the following things:
-- LDRs have a FIXED number of entries, because either
-- (a) They are all the same size and the total byte capacity is set
-- (b) They have variable size entries, but we are counting only LIST items
-- Either way, every FULL Cold Dir or Warm (LDR) data page has a known
-- number of items in it.  The only items that are unknown are the
-- partially filled Warm Top and Cold Head.  Those need to be opened to
-- get an accurate reading.
-- if( position < hotListSize ) then
--   It's a hot list position
-- elseif( position < warmListSize ) then
--   Its a warm list position
-- else
--   It's a cold list position
-- end
-- TODO:
-- (*) Track Warm and Cold List Capacity
-- (*) Track WarmTop Size (how much room is left?)
-- (*) Track ColdTop Size (how much room is left?)
--
--
-- Parms:
-- (*) topRec: Top (LDT Holding) Record
-- (*) ldtCtrl: Main LDT Control structure
-- (*) sp: searchPath Object (we will fill this in)
-- (*) position: Find this Object Position in the LSTACK
-- Return:
-- SP: Filled in with position in Stack Object.  Location is computed in
--     terms of OBJECTS (not bytes), regardless of mode.  The mode
--     (LIST or BINARY) does determine how the position  is calculated.
--  0: Success
-- -1: ERRORS
-- ======================================================================
local function locatePosition( topRec, ldtCtrl, sp, position )
  local meth = "locatePosition()";
  GP=E and trace("[ENTER]:<%s:%s> LDT(%s) Position(%d)",
    MOD, meth, ldtSummaryString( ldtCtrl ), position );

    -- TODO: Finish this later -- if needed at all.
  warn("[WARNING!!]<%s:%s> FUNCTION UNDER CONSTRUCTION!!! ", MOD, meth );

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  local directoryListPosition = 0; -- if non-zero, we're in cold list
  local digestListPosition = 0;    -- if non-zero, we're cold or warm list
  local entryListPosition = 0;     -- The place in the entry list.

  info("[NOTICE!!]<%s:%s> This is LIST MODE ONLY", MOD, meth );
  -- TODO: Must be extended for BINARY -- MODE.
  if( ldtMap[M_StoreMode] == SM_LIST ) then
    local hotListAmount = list.size( ldtMap[M_HotEntryList] );
    local warmListMax = ldtMap[M_WarmListMax];
    local warmFullCount = ldtMap[M_WarmListDigestCount] - 1;
    local warmTopEntries = ldtMap[M_WarmTopEntryCount];
    local warmListPart = (warmFullCount * warmListMax) + warmTopEntries;
    local warmListAmount = hotListPart + warmListPart;
    if( position <= hotListLimit ) then
      info("[Status]<%s:%s> In the Hot List", MOD, meth );
      -- It's a hot list position:
      entryListPosition = position;
    elseif( position <= warmListSize ) then
      info("[Status]<%s:%s> In the Warm List", MOD, meth );
      -- Its a warm list position: Subtract off the HotList portion and then
      -- calculate where in the Warm list we are.  Integer divide to locate
      -- the LDR, modulo to locate the warm List Position in the LDR
      local remaining = position - hotListAmount;
      -- digestListPosition = 
    else
      info("[Status]<%s:%s> In the Cold List", MOD, meth );
      -- It's a cold list position: Subract off the Hot and Warm List portions
      -- to isolate the Cold List part.
    end
  else
      warn("[NOTICE]<%s:%s> MUST IMPLEMENT BINARY MODE!!", MOD, meth );
      warn("[INCOMPLETE CODE] Binary Mode Not Implemented");
      error( ldte.ERR_INTERNAL );
  end
  -- TODO:
  -- (*) Track Warm and Cold List Capacity
  -- (*) Track WarmTop Size (how much room is left?)

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- locatePosition

-- ======================================================================
-- localTrim( topRec, ldtCtrl, searchPath )
-- ======================================================================
-- Release the storage that is colder than the location marked in the
-- searchPath object.
--
-- It is not (yet) clear if this needs to be an EXACT operation, or just
-- an approximate one.  We would prefer that we could release the storage
-- at the LDR (page) boundary.
-- ======================================================================
local function localTrim( topRec, ldtCtrl, searchPath )
  local meth = "localTrim()";
  GP=E and trace("[ENTER]:<%s:%s> LsoSummary(%s) SearchPath(%s)",
    MOD, meth, lsoSummaryString(ldtCtrl), tostring(searchPath));
    
  -- TODO: Finish this later -- if needed at all.
  warn("[WARNING!!]<%s:%s> FUNCTION UNDER CONSTRUCTION!!! ", MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- localTrim()


-- ========================================================================
-- This function is under construction.
-- ========================================================================
-- ========================================================================
-- lstack_delete_subrecs() -- Delete the entire lstack -- in pieces.
-- ========================================================================
-- The real delete (above) will do the correct delete, which is to remove
-- the ESR and the BIN.  THIS function is more of a test function, which
-- will remove each SUBREC individually.
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- First, fetch all of the digests of subrecords that go with this
-- LDT, then iterate thru the list and delete them.
-- Finally  -- Reset the record[lsoBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function lstack_delete_subrecs( topRec, lsoBinName )
  local meth = "lstack_delete()";

  GP=E and trace("[ENTER]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- Not sure if we'll ever use this function -- we took a different direction
  warn("[WARNING!!!]::LSTACK_DELETE_SUBRECS IS NOT YET IMPLEMENTED!!!");

  local rc = 0; -- start off optimistic

  -- Validate the lsoBinName before moving forward
  validateRecBinAndMap( topRec, lsoBinName, true );

  -- Extract the property map and lso control map from the lso bin list.
  local ldtCtrl = topRec[ lsoBinName ];
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- TODO: Create buildSubRecList()
  local deleteList = buildSubRecList( topRec, ldtCtrl );
  local listSize = list.size( deleteList );
  local digestString;
  local subrec;
  for i = 1, listSize, 1 do
      -- Open the Subrecord -- and then remove it.
      digestString = tostring( deleteList[i] );
      GP=F and trace("[SUBREC DELETE]<%s:%s> About to Open and Delete(%s)",
        MOD, meth, digestString );
      subrec = aerospike:open_subrec( topRec, digestString );
      if( subrec ~= nil ) then
        rc = aerospike:remove_subrec( subrec );
        if( rc == nil or rc == 0 ) then
          GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
        else
          warn("[SUB DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, binName);
          error( ldte.ERR_SUBREC_DELETE );
        end
      else
        warn("[ERROR]<%s:%s> Can't open Subrec: Digest(%s)", MOD, meth,
          digestString );
      end
  end -- for each subrecord
  return rc;

end -- lstack_delete_subrecs()

-- ======================================================================
-- processModule( moduleName )
-- ======================================================================
-- We expect to see several things from a user module.
-- (*) An adjust_settings() function: where a user overrides default settings
-- (*) Various filter functions (callable later during search)
-- (*) Transformation functions
-- (*) UnTransformation functions
-- The settings and transformation/untransformation are all set from the
-- adjust_settings() function, which puts these values in the control map.
-- ======================================================================
local function processModule( ldtCtrl, moduleName )
  local meth = "processModule()";
  GP=E and trace("[ENTER]<%s:%s> Process User Module(%s)", MOD, meth,
    tostring( moduleName ));

  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  local userModule = require(moduleName);
  if( userModule == nil ) then
    warn("[ERROR]<%s:%s> User Module(%s) is not valid", MOD, meth, moduleName);
  else
    local userSettings =  userModule[G_SETTINGS];
    if( userSettings ~= nil ) then
      userSettings( ldtMap ); -- hope for the best.
      ldtMap[M_UserModule] = moduleName;
    end
  end

  warn("[ERROR]<%s:%s> Mod(%s) THIS FUNCTION NOT YET IMPLEMENTED", MOD, meth,
    tostring( moduleName));

end -- processModule()

-- ======================================================================
-- || localCreate ||
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
local function localCreate( topRec, lsoBinName, createSpec )
  local meth = "localCreate()";

  GP=F and trace("\n\n >>>>>>>>> API[ LSTACK CREATE ] <<<<<<<<<< \n");

  if createSpec == nil then
    GP=E and trace("[ENTER1]: <%s:%s> lsoBinName(%s) NULL createSpec",
      MOD, meth, tostring(lsoBinName));
  else
    GP=E and trace("[ENTER2]: <%s:%s> lsoBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( lsoBinName), tostring( createSpec ));
  end

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, false );

  -- Create and initialize the LSO Object:: The List that holds both
  -- the Property Map and the LSO Map;
  -- NOTE: initializeLso() also assigns the ldtCtrl to the record bin.
  local ldtCtrl = initializeLso( topRec, lsoBinName );

  -- If the user has passed in settings that override the defaults
  -- (the createSpec), then process that now.
  if( createSpec ~= nil )then
    local createSpecType = type(createSpec);
    if( createSpecType == "string" ) then
      processModule( ldtCtrl, createSpec );
    elseif( createSpecType == "userdata" ) then
      adjustLsoList( ldtCtrl, createSpec )
    else
      warn("[WARNING]<%s:%s> Unknown Creation Object(%s)::Ignored",
        MOD, meth, tostring( createSpec ));
    end
  end

  GP=F and trace("[DEBUG]:<%s:%s>:LsoList after Init(%s)",
    MOD, meth, tostring(ldtCtrl));

  -- Update the Record.
  topRec[lsoBinName] = ldtCtrl;
  record.set_flags(topRec, lsoBinName, BF_LDT_BIN );--Must set every time

  -- All done, store the record (Create if needed, or just Update).
  local rc;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  if( rc == nil or rc == 0 ) then
    GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=E and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_CREATE );
  end
  
end -- function localCreate()
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
-- =======================================================================
local function localStackPush( topRec, lsoBinName, newValue, createSpec )
  local meth = "localStackPush()";

  GP=F and trace("\n\n >>>>>>>>> API[ LSTACK PUSH ] <<<<<<<<<< \n");

  -- Note: functionTable is "global" to this module, defined at top of file.

  GP=E and trace("[ENTER1]:<%s:%s>LSO BIN(%s) NewVal(%s) createSpec(%s)",
      MOD, meth, tostring(lsoBinName), tostring( newValue ),
      tostring( createSpec ) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, false );

  -- Create the SubrecContext, which will hold all of the open subrecords.
  -- The key will be the DigestString, and the value will be the subRec
  -- pointer.
  local src = createSubrecContext();

  -- Check for existence, and create if not there.  If we create AND there
  -- is a "createSpec", then configure this LSO appropriately.
  local ldtCtrl;
  local ldtMap;
  local propMap;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[NOTICE]:<%s:%s>:Record Does Not exist. Creating",
      MOD, meth );
    ldtCtrl = initializeLso( topRec, lsoBinName );
    if( createSpec ~= nil ) then
      -- If the CreateSpecification is present, then modify the LSO parms
      adjustLsoList( ldtCtrl, createSpec );
    end
    aerospike:create( topRec );
  elseif ( topRec[lsoBinName] == nil ) then
    GP=F and trace("[NOTICE]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating",
                   MOD, meth, tostring(lsoBinName) );
    ldtCtrl = initializeLso( topRec, lsoBinName );

    -- If the user has passed in settings that override the defaults
    -- (the createSpec), then process that now.
    if( createSpec ~= nil )then
      local createSpecType = type(createSpec);
      if( createSpecType == "string" ) then
        processModule( createSpec );
      elseif( createSpecType == "userdata" ) then
        adjustLsoList( ldtCtrl, createSpec )
      else
        warn("[WARNING]<%s:%s> Unknown Creation Object(%s)::Ignored",
          MOD, meth, tostring( createSpec ));
      end
    end
    aerospike:create( topRec );
  else
    -- if the map already exists, then we don't adjust with createSpec.
    ldtCtrl = topRec[lsoBinName];
  end
  -- Extract the Property Map and LsoMap from the LsoList;
  propMap = ldtCtrl[1];
  ldtMap  = ldtCtrl[2];
  
  -- Now, it looks like we're ready to insert.  If there is a transform
  -- function present, then apply it now.
  -- Note: functionTable is "global" to this module, defined at top of file.
  local newStoreValue;
  if ldtMap[M_Transform] ~= nil  then 
    GP=F and trace("[DEBUG]: <%s:%s> Applying Transform (%s)",
      MOD, meth, tostring(ldtMap[M_Transform] ) );
    newStoreValue = functionTable[ldtMap[M_Transform]]( newValue );
  else
    newStoreValue = newValue;
  end

  -- If we have room, do the simple list insert.  If we don't have
  -- room, then make room -- transfer half the list out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.  (Ok to use ldtMap and not ldtCtrl here).
  if hotListHasRoom( ldtMap, newStoreValue ) == false then
    GP=F and trace("[DEBUG]:<%s:%s>: CALLING TRANSFER HOT LIST!!",MOD, meth );
    hotListTransfer( src, topRec, ldtCtrl );
  end
  hotListInsert( ldtCtrl, newStoreValue );
  -- Must always assign the object BACK into the record bin.
  -- Check to see if we really need to reassign the MAP into the list as well.
  ldtCtrl[2] = ldtMap;
  topRec[lsoBinName] = ldtCtrl;
  record.set_flags(topRec, lsoBinName, BF_LDT_BIN );--Must set every time

  -- All done, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", MOD, meth );

  -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
  -- so just turn any NILs into zeros.
  local rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=E and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end
end -- function localStackPush()

-- =======================================================================
-- Local Push ALL
-- =======================================================================
-- Iterate thru the list and call localStackPush on each element
-- =======================================================================
local function localPushAll( topRec, lsoBinName, valueList, createSpec )
  local meth = "localPushAll()";
  GP=E and trace("[ENTER]:<%s:%s>LSO BIN(%s) valueList(%s) createSpec(%s)",
    MOD, meth, tostring(lsoBinName), tostring(valueList), tostring(createSpec));

  local rc = 0;
  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      rc = localStackPush( topRec, lsoBinName, valueList[i], createSpec );
      if( rc < 0 ) then
        warn("[ERROR]<%s:%s> Problem Inserting Item #(%d) [%s]", MOD, meth, i,
          tostring( valueList[i] ));
        error(ldte.ERR_INSERT);
      end
    end
  else
    warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
      MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  return rc;
end -- end localPushAll()

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
-- NOTE: July 2013:tjl: Now using the SubrecContext to track the open
-- subrecs.
-- ======================================================================
local function localStackPeek( topRec, lsoBinName, peekCount, func, fargs )
  local meth = "localStackPeek()";

  GP=F and trace("\n\n >>>>>>>>> API[ LSTACK PEEK ] <<<<<<<<<< \n");

  GP=E and trace("[ENTER]: <%s:%s> LSO BIN(%s) Count(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(lsoBinName), tostring(peekCount),
    tostring(func), tostring(fargs) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, true );
  local ldtCtrl = topRec[ lsoBinName ];
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  GP=F and trace("[DEBUG]: <%s:%s> LSO List Summary(%s)",
    MOD, meth, lsoSummaryString( ldtCtrl ) );


  -- Create the SubrecContext, which will hold all of the open subrecords.
  -- The key will be the DigestString, and the value will be the subRec
  -- pointer.
  local src = createSubrecContext();

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  
  -- When the user passes in a "peekCount" of ZERO, then we read ALL.
  -- Actually -- we will also read ALL if count is negative.
  -- New addition -- with the STORE LIMIT addition (July 2013) we now
  -- also limit our peeks to the storage limit -- which also discards
  -- storage for LDRs holding items beyond the limit.
  local all = false;
  local count = 0;
  local itemCount = propMap[PM_ItemCount];
  local storeLimit = ldtMap[M_StoreLimit];

  if( peekCount <= 0 ) then
    if( itemCount < storeLimit ) then
      all = true;
    else
      count = storeLimit; -- peek NO MORE than our storage limit.
    end
  elseif( peekCount > storeLimit ) then
    count = storeLimit;
  else
    count = peekCount;
  end

  -- Set up our answer list.
  local resultList = list(); -- everyone will fill this in

  GP=F and trace("[DEBUG]<%s:%s> Peek with Count(%d) StoreLimit(%d)",
      MOD, meth, count, storeLimit );

  -- Fetch from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local resultList = hotListRead(resultList, ldtCtrl, count, func, fargs, all);
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
  if list.size(ldtMap[M_WarmDigestList]) > 0 then
    warmCount =
     warmListRead(src,topRec,resultList,ldtCtrl,remainingCount,func,fargs,all);
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

  GP=F and trace("[DEBUG]:<%s:%s>After WarmListRead: ldtMap(%s) ldtCtrl(%s)",
    MOD, meth, tostring(ldtMap), lsoSummaryString(ldtCtrl));

  numRead = list.size( resultList );
  -- If we've read enough, then return.
  if ( (remainingCount <= 0 and all == false) or
       (numRead >= propMap[PM_ItemCount] ) )
  then
      return resultList; -- We have all we need.  Return.
  end

  -- Otherwise, go look for more in the Cold List.
  local coldCount = 
     coldListRead(src,topRec,resultList,ldtCtrl,remainingCount,func,fargs,all);

  GP=E and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    MOD, meth, peekCount, summarizeList(resultList));

  return resultList;
end -- function localStackPeek() 

-- ========================================================================
-- This function is (still) under construction
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

  GP=E and trace("[ENTER1]: <%s:%s> lsoBinName(%s) trimCount(%s)",
    MOD, meth, tostring(lsoBinName), tostring( trimCount ));

  warn("[NOTICE!!]<%s:%s> Under Construction", MOD, meth );

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  ldtCtrl = topRec[lsoBinName];

  -- Move to the location (Hot, Warm or Cold) that is the trim point.
  -- TODO: Create locatePosition()
  local searchPath = locatePosition( topRec, ldtCtrl, trimCount );

  -- From searchPath to the end, release storage.
  -- TODO: Create localTrim()
  localTrim( topRec, ldtCtrl, searchPath );

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );

  return config;
end -- function lstack_trim()

-- ========================================================================
-- localGetSize() -- return the number of elements (item count) in the stack.
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
local function localGetSize( topRec, lsoBinName )
  local meth = "localGetSize()";

  GP=E and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local ldtCtrl = topRec[ lsoBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];
  local itemCount = propMap[PM_ItemCount];
  local storeLimit = ldtMap[M_StoreLimit];

  -- Note that itemCount should never appear larger than the storeLimit,
  -- but until our internal accounting is fixed, we fudge it like this.
  if( itemCount > storeLimit ) then
      itemCount = storeLimit;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function localGetSize()

-- ========================================================================
-- localGetCapacity() -- return the current capacity setting for LSTACK.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
local function localGetCapacity( topRec, lsoBinName )
  local meth = "localGetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local ldtCtrl = topRec[ lsoBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local ldtMap = ldtCtrl[2];
  local capacity = ldtMap[M_StoreLimit];

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function localGetCapacity()

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
local function localConfig( topRec, lsoBinName )
  local meth = "localConfig()";

  GP=E and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local ldtCtrl = topRec[ lsoBinName ];
  local config = lsoSummary( ldtCtrl );

  GP=E and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config));

  return config;
end -- function localConfig()


-- ========================================================================
-- This function is (still) under construction.
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

  GP=E and trace("[ENTER]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- Extract the property map and lso control map from the lso bin list.
  local ldtCtrl = topRec[ lsoBinName ];
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Copy the warm list into the result list
  local wdList = ldtMap[M_WarmDigestList];
  local transAmount = list.size( wdList );
  local resultList = list.take( wdList, transAmount );

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(ldtMap[M_ColdDirListHead] == nil or ldtMap[M_ColdDirListHead] == 0) then
    return resultList;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirRecDigest = ldtMap[M_ColdDirListHead];

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

  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList
end -- lstack_subrec_list()

-- ========================================================================
-- localLdtRemove() -- Remove the LDT entirely from the record.
-- NOTE: This could eventually be moved to COMMON, and be "localLdtRemove()",
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
local function localLdtRemove( topRec, binName )
  local meth = "localLdtRemove()";

  GP=F and trace("\n\n >>>>>>>>> API[ LSTACK REMOVE ] <<<<<<<<<< \n");

  GP=E and trace("[ENTER]<%s:%s> binName(%s)", MOD, meth, tostring(binName));
  local rc = 0; -- start off optimistic

  -- Validate the binName before moving forward
  validateRecBinAndMap( topRec, binName, true );

  -- Extract the property map and lso control map from the lso bin list.
  local ldtList = topRec[ binName ];
  local propMap = ldtList[1];

  -- Get the ESR and delete it -- if it exists.  If we have ONLY a HotList,
  -- then the ESR will be ZERO.
  local esrDigest = propMap[PM_EsrDigest];
  if( esrDigest ~= nil and esrDigest ~= 0 ) then
    local esrDigestString = tostring(esrDigest);
    GP=f and trace("[SUBREC OPEN]<%s:%s> Digest(%s)",MOD,meth,esrDigestString);
    local esrRec = aerospike:open_subrec( topRec, esrDigestString );
    if( esrRec ~= nil ) then
      rc = aerospike:remove_subrec( esrRec );
      if( rc == nil or rc == 0 ) then
        GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
      else
        warn("[ESR DELETE ERROR]<%s:%s>RC(%d) Bin(%s)", MOD, meth, rc, binName);
        error( ldte.ERR_SUBREC_DELETE );
      end
    else
      warn("[ESR DELETE ERROR]<%s:%s> ERROR on ESR Open", MOD, meth );
    end
  else
    info("[INFO]<%s:%s> LDT ESR is not yet set, so remove not needed. Bin(%s)",
      MOD, meth, binName );
  end

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin(%s) invalid",
      MOD, meth, REC_LDT_CTRL_BIN );
    error( ldte.ERR_INTERNAL );
  end
  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- Remove this bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    -- Set this control bin as HIDDEN
    record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  end

  -- Null out the LDT bin and update the record.
  topRec[binName] = nil;

  -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
  -- so just turn any NILs into zeros.
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=E and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end
end -- localLdtRemove()

-- ========================================================================
-- localSetCapacity()
-- ========================================================================
-- This is a special command to both set the new storage limit.  It does
-- NOT release storage, however.  That is done either lazily after a 
-- warm/cold insert or with an explit lstack_trim() command.
-- Parms:
-- (*) topRec: the user-level record holding the LSO Bin
-- (*) lsoBinName: The name of the LSO Bin
-- (*) newLimit: The new limit of the number of entries
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function localSetCapacity( topRec, lsoBinName, newLimit )
  local meth = "localSetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> lsoBinName(%s) newLimit(%s)",
    MOD, meth, tostring(lsoBinName), tostring(newLimit));

  local rc = 0; -- start off optimistic

  -- Validate user parameters
  if( type( newLimit ) ~= "number" or newLimit <= 0 ) then
    warn("[PARAMETER ERROR]<%s:%s> newLimit(%s) must be a positive number",
      MOD, meth, tostring( newLimit ));
    error( ldte.ERR_INPUT_PARM );
  end

  -- Validate the lsoBinName before moving forward
  validateRecBinAndMap( topRec, lsoBinName, true );

  -- Extract the property map and lso control map from the lso bin list.
  local ldtCtrl = topRec[ lsoBinName ];
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  GP=F and trace("[LSO SUMMARY]: <%s:%s> Summary(%s)", MOD, meth,
    lsoSummaryString( ldtCtrl ));

  info("[PARAMETER UPDATE]<%s:%s> StoreLimit: Old(%d) New(%d) ItemCount(%d)",
    MOD, meth, ldtMap[M_StoreLimit], newLimit, propMap[PM_ItemCount] );

  -- Update the LSO Control map with the new storage limit
  ldtMap[M_StoreLimit] = newLimit;

  -- Use the new "Limit" to compute how this affects the storage parameters.
  -- Basically, we want to determine how many Cold List directories this
  -- new limit equates to.  Then, if we add more than that many cold list
  -- directories, we'll release that storage.
  -- Note: We're doing this in terms of LIST mode, not yet in terms of
  -- binary mode.  We must compute this in terms of MINIMAL occupancy, so
  -- that we always have AT LEAST "Limit" items in the stack.  Therefore,
  -- we compute Hotlist as minimal size (max - transfer) as well as WarmList
  -- (max - transfer)
  -- Total Space comprises:
  -- (*) Hot List Storage
  -- ==> (HotListMax - HotListTransfer)  Items (minimum)
  -- (*) Warm List Storage
  -- ==> ((WarmListMax - WarmListTransfer) * LdrEntryCountMax)
  -- (*) ColdList (one Cold Dir) Capacity -- because there is no limit to
  --     the number of coldDir Records we can have.
  -- ==> ColdListMax * LdrEntryCountMax
  --
  -- So -- if we set the limit to 10,000 items and all of our parameters
  -- are set to 100:
  -- HotListMax = 100
  -- HotListTransfer = 50
  -- WarmListMax = 100
  -- WarmListTransfer = 50
  -- LdrEntryCountMax = 100
  -- ColdListMax = 100
  --
  -- Then, our numbers look like this:
  -- (*) Hot Storage (between 50 and 100 data elements)
  -- (*) LDR Storage (100 elements) (per Warm or Cold Digest)
  -- (*) Warm Storage (between 5,000 and 10,000 elements)
  -- (*) Cold Dir Storage ( between 5,000 and 10,000 elements for the FIRST
  --     Cold Dir (the head), and 10,000 elements for every Cold Dir after
  --     that.
  --
  -- So, a limit of 75 would keep all storage in the hot list, with a little
  -- overflow into the warm List.  An Optimal setting would set the
  -- Hot List to 100 and the transfer amount to 25, thus guaranteeing that
  -- the HotList always contained the desired top 75 elements.  However,
  -- we expect capacity numbers to be in the thousands, not tens.
  --
  -- A limit of 1,000 would limit Warm Storage to 10 (probably 10+1)
  -- warm list digest cells.
  --
  -- A limit of 10,000 would limit the Cold Storage to a single Dir list,
  -- which would release "transfer list" amount of data when that much more
  -- was coming in.
  --
  -- A limit of 20,000 would limit Cold Storage to 2:
  -- 50 Hot, 5,000 Warm, 15,000 Cold.
  --
  -- For now, we're just going to release storage at the COLD level.  So,
  -- we'll basically compute a stairstep function of how many Cold Directory
  -- records we want to use, based on the system parameters.
  -- Under 10,000:  1 Cold Dir
  -- Under 20,000:  2 Cold Dir
  -- Under 50,000:  5 Cold Dir
  
  local hotListMin = ldtMap[M_HotListMax] - ldtMap[M_HotListTransfer];
  local ldrSize = ldtMap[M_LdrEntryCountMax];
  local warmListMin =
    (ldtMap[M_WarmListMax] - ldtMap[M_WarmListTransfer]) * ldrSize;
  local coldListSize = ldtMap[M_ColdListMax];
  local coldGranuleSize = ldrSize * coldListSize;
  local coldRecsNeeded = 0;
  if( newLimit < (hotListMin + warmListMin) ) then
    coldRecsNeeded = 0;
  elseif( newLimit < coldGranuleSize ) then
    coldRecsNeeded = 1;
  else
    coldRecsNeeded = math.ceil( newLimit / coldGranuleSize );
  end

  GP=F and trace("[STATUS]<%s:%s> Cold Granule(%d) HLM(%d) WLM(%d)",
    MOD, meth, coldGranuleSize, hotListMin, warmListMin );
  GP=F and trace("[UPDATE]:<%s:%s> New Cold Rec Limit(%d)", MOD, meth, 
    coldRecsNeeded );

  ldtMap[M_ColdDirRecMax] = coldRecsNeeded;
  topRec[lsoBinName] = ldtCtrl; -- ldtMap is implicitly included.
  record.set_flags(topRec, lsoBinName, BF_LDT_BIN );--Must set every time

  -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
  -- so just turn any NILs into zeros.
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
    return 0;
  else
    GP=E and trace("[ERROR EXIT]:<%s:%s> Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end
end -- localSetCapacity();

-- ========================================================================
-- localDebug() -- Turn the debug setting on (1) or off (0)
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- It would be nice if we could figure out how to make this setting change
-- PERSISTENT. Until we do that, this will be a no-op.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function localDebug( topRec, setting )
  local meth = "localDebug()";
  local rc = 0;

  GP=E and trace("[ENTER]: <%s:%s> setting(%s)", MOD, meth, tostring(setting));
  if( setting ~= nil and type(setting) == "number" ) then
    if( setting == 1 ) then
      info("[DEBUG SET]<%s:%s> Turn Debug ON", MOD, meth );
      F = true;
    elseif( setting == 0 ) then
      info("[DEBUG SET]<%s:%s> Turn Debug OFF", MOD, meth );
      F = false;
    else
      info("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
      rc = -1;
    end
  else
    info("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
    rc = -1;
  end
  return rc;
end -- localDebug()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSTACK External Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Notice the namechange -- we're no longer using the name convention:
-- lstack_xxx(), e.g. lstack_create()
-- we're now using just the name without the "lstack_" prefix.
-- So, lstack_create() now becomes create().
-- ======================================================================

-- ======================================================================
-- || create        ||
-- || lstack_create ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
--
-- For this version of lstack, we will be using a LIST of two maps,
-- which contain lots of metadata, plus one list:
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
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function create( topRec, lsoBinName, createSpec )
  return localCreate( topRec, lsoBinName, createSpec );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_create( topRec, lsoBinName, createSpec )
  return localCreate( topRec, lsoBinName, createSpec );
end

-- =======================================================================
-- push()
-- lstack_push()
-- =======================================================================
-- Push a value on the stack, with the optional parm to set the LDT
-- configuration in case we have to create the LDT before calling the push.
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec )
end -- push()

function create_and_push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec );
end -- create_and_push()

-- OLD EXTERNAL FUNCTIONS
function lstack_push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec )
end -- end lstack_push()

function lstack_create_and_push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec );
end -- lstack_create_and_push()

-- =======================================================================
-- Stack Push ALL
-- =======================================================================
-- Iterate thru the list and call localStackPush on each element
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push_all( topRec, lsoBinName, valueList, createSpec )
  return localPushAll( topRec, lsoBinName, valueList, createSpec )
end

-- OLD EXTERNAL FUNCTIONS
function lstack_push_all( topRec, lsoBinName, valueList, createSpec )
  return localPushAll( topRec, lsoBinName, valueList, createSpec )
end

-- =======================================================================
-- lstack_peek() -- with and without filters
-- peek() -- with and without filters
--
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function peek( topRec, lsoBinName, peekCount )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- peek()

function peek_then_filter( topRec, lsoBinName, peekCount, func, fargs )
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- peek_then_filter()

-- OLD EXTERNAL FUNCTIONS
function lstack_peek( topRec, lsoBinName, peekCount )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- lstack_peek()

function lstack_peek_then_filter( topRec, lsoBinName, peekCount, func, fargs )
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- lstack_peek_then_filter()

-- ========================================================================
-- get_size() -- return the number of elements (item count) in the stack.
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
-- NEW EXTERNAL FUNCTIONS
function size( topRec, lsoBinName )
  return localGetSize( topRec, lsoBinName );
end -- function size()

function get_size( topRec, lsoBinName )
  return localGetSize( topRec, lsoBinName );
end -- function get_size()

-- OLD EXTERNAL FUNCTIONS
function lstack_size( topRec, lsoBinName )
  return localGetSize( topRec, lsoBinName );
end -- function get_size()

-- ========================================================================
-- get_capacity() -- return the current capacity setting for LSTACK.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function get_capacity( topRec, lsoBinName )
  return localGetCapacity( topRec, lsoBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_get_capacity( topRec, lsoBinName )
  return localGetCapacity( topRec, lsoBinName );
end

-- ========================================================================
-- get_config() -- return the lstack config settings.
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
-- NEW EXTERNAL FUNCTIONS
function get_config( topRec, lsoBinName )
  return localConfig( topRec, lsoBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_config( topRec, lsoBinName )
  return localConfig( topRec, lsoBinName );
end

-- ========================================================================
-- lstack_remove() -- Remove the LDT entirely from the record.
-- remove() -- Remove the LDT entirely from the record.
--
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
-- (2) binName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function remove( topRec, lsoBinName )
  return localLdtRemove( topRec, lsoBinName );
end -- lstack_remove()

-- OLD EXTERNAL FUNCTIONS
function lstack_remove( topRec, lsoBinName )
  return localLdtRemove( topRec, lsoBinName );
end -- lstack_remove()

function ldt_remove( topRec, lsoBinName )
  return localLdtRemove( topRec, lsoBinName );
end -- ldt_remove()
-- ========================================================================
-- lstack_set_storage_limit()
-- lstack_set_capacity()
-- set_storage_limit()
-- set_capacity()
-- ========================================================================
-- This is a special command to both set the new storage limit.  It does
-- NOT release storage, however.  That is done either lazily after a 
-- warm/cold insert or with an explit lstack_trim() command.
-- Parms:
-- (*) topRec: the user-level record holding the LSO Bin
-- (*) lsoBinName: The name of the LSO Bin
-- (*) newLimit: The new limit of the number of entries
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function lstack_set_capacity( topRec, lsoBinName, newLimit )
  return localSetCapacity( topRec, lsoBinName, newLimit );
end

function set_capacity( topRec, lsoBinName, newLimit )
  return localSetCapacity( topRec, lsoBinName, newLimit );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_set_storage_limit( topRec, lsoBinName, newLimit )
  return localSetCapacity( topRec, lsoBinName, newLimit );
end

function set_storage_limit( topRec, lsoBinName, newLimit )
  return localSetCapacity( topRec, lsoBinName, newLimit );
end

-- ========================================================================
-- lstack_debug() -- Turn the debug setting on (1) or off (0)
-- debug()        -- Turn the debug setting on (1) or off (0)
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- It would be nice if we could figure out how to make this setting change
-- PERSISTENT. Until we do that, this will be a no-op.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function set_debug( topRec, setting )
  return localDebug( topRec, setting );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_debug( topRec, setting )
  return localDebug( topRec, setting );
end

-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
