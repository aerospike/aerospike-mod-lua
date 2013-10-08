-- AS Large Set (LSET) Operations
-- Track the date and iteration of the last update.
local MOD="lset_2013_10_07.a"; 

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.1;

-- Large Set Design/Architecture
--
-- Large Set includes two different implementations in the same module.
-- The implementation used is determined by the setting "SetTypeStore"
-- in the LDT control structure.  There are the following choices.
-- (1) "TopRecord" SetTypeStore, which holds all data in the top record.
--     This is appropriate for small to medium lists only, as the total
--     storage capacity is limited to the max size of a record, which 
--     defaults to 128kb, but can be upped to 2mb (or even more?)
-- (2) "SubRecord" SetTypeStore, which holds data in sub-records.  With
--     the sub-record type, Large Sets can be virtually any size, although
--     the digest directory in the TopRecord can potentially grow large 
--     for VERY large sets.
--
-- The LDT bin value in a top record, known as "ldtCtrl" (LDT Control),
-- is a list of two maps.  The first map is the property map, and is the
-- same for every LDT.  It is done this way so that the LDT code in
-- the Aerospike Server can read any LDT property using the same mechanism.
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
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=true; -- Set B (Banners) to true to turn ON Banner Print

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSET Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSET module:
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) Object = get( topRec, ldtBinName, searchValue ) 
-- (*) Number = exists( topRec, ldtBinName, searchValue ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Object = take( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================

-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSET Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two different implementations of Large Set:
-- (1) TopRecord Mode (the first one to be implemented)
-- (2) SubRecord Mode
--
-- ++=================++
-- || TOP RECORD MODE ||
-- ++=================++
-- In a user record, the bin holding the Large SET control information
-- is named with the user's name, however, there is a restriction that there
-- can be only ONE "TopRecord Mode" LSET instance in a record.  This
-- restriction exists because of the way the numbered bins are used in the
-- record.  The Numbered LSETS bins each contain a list of entries. They
-- are prefixed with "LSetBin_" and numbered from 0 to N, where N is the
-- modulo value that is set on create (otherwise, default to 31).
--
-- When an LDT instance is created in a record, a hidden "LDT Properties" bin
-- is created in the record.  There is only ONE LDT Prop bin per record, no
-- matter how many LDT instances are in the record.
--
-- In the bin named by the user for the LSET LDT instance, (e.g. MyURLs)
-- there is an "LDT Control" structure, which is a list of two maps.
-- The first map, the Property Map, holds the same information for every
-- LDT (stack, list, map, set).  The second map holds LSET-specific 
-- information.
--
-- (Standard Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User | LDT |LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2| Prop|CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |     |           |                   
--                      V     V     V           V                   
--                   +=====++===+ +===+       +===+                  
--                   |P Map||val| |val|       |val|
--                   +-----+|val| |val|       |val|
--                   |C Map||...| |...|       |...|
--                   +=====+|val| |val|       |val|
--                          +===+ +===+       +===+ 
--
-- The Large Set distributes searches over N lists.  Searches are done
-- with linear scan in one of the bin lists.  The set values are hashed
-- and then the specific bin is picked "hash(val) Modulo N".  The N bins
-- are organized by name using the method:  prefix "LsetBin_" and the
-- modulo N number.
--
-- The modulo number is always a prime number -- to minimize the amount
-- of "collisions" that are often found in power of two modulo numbers.
-- Common choices are 17, 31 and 61.
--
-- The initial state of the LSET is "Compact Mode", which means that ONLY
-- ONE LIST IS USED -- in Bin 0.  Once there are a "Threshold Number" of
-- entries, the Bin 0 entries are rehashed into the full set of bin lists.
-- Note that a more general implementation could keep growing, using one
-- of the standard "Linear Hashing-style" growth patterns.
--
-- (Compact Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User | LDT |LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2| Prop|CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |   
--                      V     V  
--                   +=====++===+
--                   |P Map||val|
--                   +-----+|val|
--                   |C Map||...|
--                   +=====+|val|
--                          +===+
-- ++=================++
-- || SUB RECORD MODE ||
-- ++=================++
--
-- (Standard Mode)
-- +-----+-----+-----+-----+-----+
-- |User |User | LDT |LSET |User |
-- |Bin 1|Bin 2| Prop|CTRL |Bin N|
-- +-----+-----+-----+-----+-----+
--                      |    
--                      V    
--                   +======+
--                   |P Map |
--                   +------+
--                   |C Map |
--                   +------+
--                   |@|@||@| Hash Cell Anchors (Control + Digest List)
--                   +|=|=+|+
--                    | |  |                       SubRec N
--                    | |  +--------------------=>+--------+
--                    | |              SubRec 2   |Entry 1 |
--                    | +------------=>+--------+ |Entry 2 |
--                    |      SubRec 1  |Entry 1 | |   o    |
--                    +---=>+--------+ |Entry 2 | |   o    |
--                          |Entry 1 | |   o    | |   o    |
--                          |Entry 2 | |   o    | |Entry n |
--                          |   o    | |   o    | +--------+
--                          |   o    | |Entry n |
--                          |   o    | +--------+
--                          |Entry n | "LDR" (LDT Data Record) Pages
--                          +--------+ hold the actual data entries
--
-- The Hash Directory actually has a bit more structure than just a 
-- digest list. The Hash Directory is a list of "Cell Anchors" that
-- contain several pieces of information.
-- A Cell Anchor maintains a count of the subrecords attached to a
-- a hash directory entry.  Furthermore, if there are more than one
-- subrecord associated with a hash cell entry, then we basically use
-- the hash value to further distribute the values across multiple
-- sub-records.
--
--  +-------------+
-- [[ Cell Anchor ]]
--  +-------------+
-- Cell Item Count: Total number of items in this hash cell
-- Cell SubRec Count: Number of sub-records associated with this hash cell
-- Cell SubRec Depth: Depth (modulo) of the rehash (1, 2, 4, 8 ...)
-- Cell Digest Map: The association of a hash depth value to a digest
-- Cell Item List: If in "bin compact mode", the list of elements.
--
-- ======================================================================
-- Aerospike Server Functions:
-- The following functions are used to manipulate TopRecords and
-- SubRecords.
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
--
-- ++==================++
-- || External Modules ||
-- ++==================++
-- set up our "outside" links.
-- We use this to get our Hash Functions
local  CRC32 = require('ldt/CRC32');

-- We use this to get access to all of the Functions
local functionTable = require('ldt/UdfFunctionTable');

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt/ldt_errors');

-- We have a set of packaged settings for each LDT.
local lsetPackage = require('ldt/settings_lset');

-- ++=======================================++
-- || GLOBAL VALUES -- Local to this module ||
-- ++=======================================++
-- This flavor of LDT (only LSET defined here)
local LDT_TYPE_LSET   = "LSET";

-- In this early version of SET, we distribute values among lists that we
-- keep in the top record.  This is the default modulo value for that list
-- distribution.   Later we'll switch to a more robust B+ Tree version.
local DEFAULT_DISTRIB = 31;
-- Switch from a single list to distributed lists after this amount
local DEFAULT_THRESHOLD = 20;

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

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)
--
-- HashType (HT) values
local HT_STATIC  ='S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC ='D'; -- Use a DYNAMIC set of bins for hash lists

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records
local ST_HYBRID = 'H'; -- Store values (lists) Hybrid Style
-- NOTE: Hybrid style means that we'll use sub-records, but for any hash
-- value that is less than "SUBRECORD_THRESHOLD", we'll store the value(s)
-- in the top record.  It is likely that very short lists will waste a lot
-- of sub-record storage. Although, storage in the top record also costs
-- in terms of the read/write of the top record.

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
-- local LSET_CONTROL_BIN       = "LSetCtrlBin";
local LSET_CONTROL_BIN       = "DO NOT USE";
local LSET_DATA_BIN_PREFIX   = "LSetBin_";

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

-- Errors used in LDT Land, but errors returned to the user are taken
-- from the common error module: ldt_errors.lua
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

-- -----------------------------------------------------------------------
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
local PM_SubRecCount           = 'S'; -- (Top): # of subRecs in the LDT
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_CreateTime			   = 'C'; -- (Top): LDT Create Time
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
local M_UserModule             = 'P'; -- User's Lua file for overrides
local M_Transform              = 't'; -- Transform object to Binary form
local M_UnTransform            = 'u'; -- UnTransform object from Binary form
local M_LdrEntryCountMax       = 'e'; -- Max size of the LDR List
local M_LdrByteEntrySize       = 's'; -- Size of a Fixed Byte Object
local M_LdrByteCountMax        = 'b'; -- Max Size of the LDR in bytes
local M_StoreState             = 'S'; -- Store State (Compact or List)
local M_SetTypeStore           = 'T'; -- Type of the Set Store (Rec/SubRec)
local M_HashType               = 'h'; -- Hash Type (static or dynamic)
local M_BinaryStoreSize        = 'B'; -- Size of Object when in Binary form
local M_KeyType                = 'K'; -- Key Type: Atomic or Complex
local M_TotalCount             = 'C'; -- Total number of slots used
local M_Modulo 				   = 'm'; -- Modulo used for Hash Function
local M_ThreshHold             = 'H'; -- Threshold: Compact->Regular state
local M_KeyFunction            = 'F'; -- User Supplied Key Extract Function
local M_CompactList            = 'c'; -- Compact List (when in Compact Mode)
local M_HashDirectory          = 'D'; -- Directory of Hash Cells
local M_BinListThreshold       = 'l'; -- Threshold for converting from a
                                      -- local binlist to sub-record.
-- ------------------------------------------------------------------------
-- Maintain the LSET letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
--
-- A:                         a:                         0:
-- B:M_BinaryStoreSize        b:M_LdrByteCountMax        1:
-- C:M_TotalCount             c:M_CompactList            2:
-- D:M_HashDirectory          d:                         3:
-- E:                         e:M_LdrEntryCountMax       4:
-- F:M_KeyFunction            f:                         5:
-- G:                         g:                         6:
-- H:M_Threshold              h:M_HashType               7:
-- I:                         i:                         8:
-- J:                         j:                         9:
-- K:M_KeyType                k:
-- L:M_StoreLimit             l:M_BinListThreshold
-- M:M_StoreMode              m:M_Modulo
-- N:                         n:
-- O:                         o:
-- P:M_UserModule             p:
-- Q:                         q:
-- R:                         r:                     
-- S:M_StoreState             s:M_LdrByteEntrySize   
-- T:M_SetTypeStore           t:M_Transform
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
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ------------------------------------------------------------------------
-- For the Sub-Record version of Large Set, we store the values for a
-- particular hash cell in one or more sub-records.  For a given modulo
-- (directory size) value, we allocate a small object that holds the anchor
-- for one or more sub-records that hold values.
-- ------------------------------------------------------------------------
-- ++==================++
-- || Hash Cell Anchor ||
-- ++==================++
local X_ItemCount              = 'I'; -- Number of items for this dir cell
local X_SubRecordCount         = 'S'; -- Number of sub recs for this dir cell
local X_DigestHead             = 'D'; -- Head of the Sub-Rec list
local X_ListHead               = 'L'; -- Direct list (if not a Sub-Rec list)

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT Data Record (LDR) Control Map Fields (Recall that each Map ALSO has
-- the PM (general property map) fields.
local LDR_ByteEntryCount       = 'C'; -- Count of bytes used (in binary mode)
local LDR_NextSubRecDigest     = 'N'; -- Digest of Next Subrec in the chain

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

-- There are THREE different types of (Child) sub-records that are associated
-- with an LSTACK LDT:
-- (1) LDR (Lso Data Record) -- used in both the Warm and Cold Lists
-- (2) ColdDir Record -- used to hold lists of LDRs (the Cold List Dirs)
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT sub-records have a properties bin that holds a map that defines
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

-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================
-- We have several different situations where we need to look up a user
-- defined function:
-- (*) Object Transformation (e.g. compression)
-- (*) Object UnTransformation
-- (*) Predicate Filter (perform additional predicate tests on an object)
--
-- These functions are passed in by name (UDF name, Module Name), so we
-- must check the existence/validity of the module and UDF each time we
-- want to use them.  Furthermore, we want to centralize the UDF checking
-- into one place -- so on entry to those LDT functions that might employ
-- these UDFs (e.g. insert, filter), we'll set up either READ UDFs or
-- WRITE UDFs and then the inner routines can call them if they are
-- non-nil.
-- ======================================================================
local G_Filter = nil;
local G_Transform = nil;
local G_UnTransform = nil;
local G_FunctionArgs = nil;
local G_KeyFunction = nil;

-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- -----------------------------------------------------------------------
-- resetPtrs()
-- -----------------------------------------------------------------------
-- Reset the UDF Ptrs to nil.
-- -----------------------------------------------------------------------
local function resetUdfPtrs()
  G_Filter = nil;
  G_Transform = nil;
  G_UnTransform = nil;
  G_FunctionArgs = nil;
  G_KeyFunction = nil;
end -- resetPtrs()

-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- -----------------------------------------------------------------------
-- setKeyFunction()
-- -----------------------------------------------------------------------
-- The function that extracts a key value from a complex object can
-- be in the user's "creation" module, or it can be in the FunctionTable.
-- The "Key" Function may be slightly misleading, depending on the LDT
-- that is being used.
-- (*) LSET: The KeyFunction extracts a unique subset from a complex object
--           that can be compared (equals only). For LSET, a KeyFunction is
--           not required, as a complex object can always be converted to a
--           string for an equals compare.
-- (*) LMAP: The KeyFunction is not used, since values are found with "name",
--           which must be an atomic (number or string) value.
-- (*) LLIST: The KeyFunction extracts an atomic value from a complex object
--            that can be ordered.  For LLIST, if the object being stored is
--            complex, then it is REQUIRED that there is a valid KeyFunction
--            to extract an atomic value that can be compared and ordered.
--            The type of the FIRST INSERT determines the type of the LLIST.
-- (*) LSTACK: For regular LSTACK, there is no need for a KeyFunction.
--            However, for TIMESTACK, a special flavor of LSTACK, the 
--            KeyFunction extracts a TIME value from the object, which must
--            be a number that can be used in an ordered compare.
-- Parms:
-- (*) ldtMap: The basic control info
-- (*) required: True when we must have a valid KeyFunction, such as for
--               LLIST.
-- -----------------------------------------------------------------------
local function setKeyFunction( ldtMap, required )
  local meth = "setKeyFunction()";
  -- Look in the Create Module first, then check the Function Table.
  local createModule = ldtMap[M_UserModule];
  local keyFunction = ldtMap[M_KeyFunction];
  G_KeyFunction = nil;
  if( keyFunction ~= nil ) then
    if( type(keyFunction) ~= "string" or filter == "" ) then
      warn("[ERROR]<%s:%s> Bad KeyFunction Name: type(%s) filter(%s)",
        MOD, meth, type(filter), tostring(filter) );
      error( ldte.ERR_KEY_FUN_BAD );
    else
      -- Ok -- so far, looks like we have a valid key function name, 
      -- Look in the Create Module, and if that's not found, then look
      -- in the system function table.
      if( G_KeyFunction == nil and createModule ~= nil ) then
        local createModuleRef = require(createModule);
        if( createModuleRef ~= nil and createModuleRef[filter] ~= nil ) then
          G_KeyFunction = createModuleRef[keyFunction];
        end
      end

      -- Last we try the UdfFunctionTable, In case the user wants to employ
      -- one of the standard Key Functions.
      if( G_KeyFunction == nil and functionTable ~= nil ) then
        G_KeyFunction = functionTable[keyFunction];
      end

      -- If we didn't find anything, BUT the user supplied a function name,
      -- then we have a problem.  We have to complain.
      if( G_KeyFunction == nil ) then
        warn("[ERROR]<%s:%s> KeyFunction not found: type(%s) KeyFunction(%s)",
          MOD, meth, type(keyFunction), tostring(keyFunction) );
        error( ldte.ERR_KEY_FUN_NOT_FOUND );
      end
    end
  elseif( required == true ) then
    warn("[ERROR]<%s:%s> Key Function is Required for LDT Complex Objects",
      MOD, meth );
    error( ldte.ERR_KEY_FUN_NOT_FOUND );
  end
end -- setKeyFunction()

-- -----------------------------------------------------------------------
-- setReadFunctions()()
-- -----------------------------------------------------------------------
-- Set the Filter and UnTransform Function pointers for Reading values.
-- We follow this hierarchical lookup pattern for the read filter function:
-- (*) User Supplied Module (might be different from create module)
-- (*) Create Module
-- (*) UdfFunctionTable
--
-- We follow this lookup pattern for the UnTransform function:
-- (*) Create Module
-- (*) UdfFunctionTable
-- Notice that it would be generally dangerous to use some sort of ad hoc
-- UnTransform filter -- the Transform/UnTransform should be defined at
-- the LDT Instance Creation, and then left alone.
--
-- -----------------------------------------------------------------------
local function setReadFunctions( ldtMap, userModule, filter, filterArgs )
  local meth = "setReadFunctions()";
  GP=E and trace("[ENTER]<%s:%s> Process Filter(%s)",
    MOD, meth, tostring(filter));

  -- Do the Filter First. If not nil, then process.  Complain if things
  -- go badly.
  local createModule = ldtMap[M_UserModule];
  G_Filter = nil;
  G_FunctionArgs = filterArgs;
  if( filter ~= nil ) then
    if( type(filter) ~= "string" or filter == "" ) then
      warn("[ERROR]<%s:%s> Bad filter Name: type(%s) filter(%s)",
        MOD, meth, type(filter), tostring(filter) );
      error( ldte.ERR_FILTER_BAD );
    else
      -- Ok -- so far, looks like we have a valid filter name, 
      if( userModule ~= nil and type(userModule) == "string" ) then
        local userModuleRef = require(userModule);
        if( userModuleRef ~= nil and userModuleRef[filter] ~= nil ) then
          G_Filter = userModuleRef[filter];
        end
      end
      -- If we didn't find a good filter, keep looking.  Try the createModule.
      if( G_Filter == nil and createModule ~= nil ) then
        local createModuleRef = require(createModule);
        if( createModuleRef ~= nil and createModuleRef[filter] ~= nil ) then
          G_Filter = createModuleRef[filter];
        end
      end
      -- Last we try the UdfFunctionTable, In case the user wants to employ
      -- one of the standard Functions.
      if( G_Filter == nil and functionTable ~= nil ) then
        G_Filter = functionTable[filter];
      end

      -- If we didn't find anything, BUT the user supplied a function name,
      -- then we have a problem.  We have to complain.
      if( G_Filter == nil ) then
        warn("[ERROR]<%s:%s> filter not found: type(%s) filter(%s)",
          MOD, meth, type(filter), tostring(filter) );
        error( ldte.ERR_FILTER_NOT_FOUND );
      end
    end
  end -- if filter not nil

  -- That wraps up the Filter handling.  Now do  the UnTransform Function.
  local untrans = ldtMap[M_UnTransform];
  G_UnTransform = nil;
  if( untrans ~= nil ) then
    if( type(untrans) ~= "string" or untrans == "" ) then
      warn("[ERROR]<%s:%s> Bad UnTransformation Name: type(%s) function(%s)",
        MOD, meth, type(untrans), tostring(untrans) );
      error( ldte.ERR_UNTRANS_FUN_BAD );
    else
      -- Ok -- so far, looks like we have a valid untransformation func name, 
      if( createModule ~= nil ) then
        local createModuleRef = require(createModule);
        if( createModuleRef ~= nil and createModuleRef[untrans] ~= nil ) then
          G_UnTransform = createModuleRef[untrans];
        end
      end
      -- Last we try the UdfFunctionTable, In case the user wants to employ
      -- one of the standard Functions.
      if( G_UnTransform == nil and functionTable ~= nil ) then
        G_UnTransform = functionTable[untrans];
      end

      -- If we didn't find anything, BUT the user supplied a function name,
      -- then we have a problem.  We have to complain.
      if( G_UnTransform == nil ) then
        warn("[ERROR]<%s:%s> UnTransform Func not found: type(%s) Func(%s)",
          MOD, meth, type(untrans), tostring(untrans) );
        error( ldte.ERR_UNTRANS_FUN_NOT_FOUND );
      end
    end
  end -- if untransform not nil

  GP=E and trace("[EXIT]<%s:%s>", MOD, meth );
end -- setReadFunctions()


-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- -----------------------------------------------------------------------
-- setWriteFunctions()()
-- -----------------------------------------------------------------------
-- Set the Transform Function pointer for Writing values.
-- We follow a hierarchical lookup pattern for the transform function.
-- (*) Create Module
-- (*) UdfFunctionTable
--
-- -----------------------------------------------------------------------
local function setWriteFunctions( ldtMap )
  local meth = "setWriteFunctions()";
  GP=E and trace("[ENTER]<%s:%s> ldtMap(%s)", MOD, meth, tostring(ldtMap));

  -- Look in the create module first, then the UdfFunctionTable to find
  -- the transform function (if there is one).
  local createModule = ldtMap[M_UserModule];
  local trans = ldtMap[M_Transform];
  G_Transform = nil;
  if( trans ~= nil ) then
    if( type(trans) ~= "string" or trans == "" ) then
      warn("[ERROR]<%s:%s> Bad Transformation Name: type(%s) function(%s)",
        MOD, meth, type(trans), tostring(trans) );
      error( ldte.ERR_TRANS_FUN_BAD );
    else
      -- Ok -- so far, looks like we have a valid transformation func name, 
      if( createModule ~= nil ) then
        local createModuleRef = require(createModule);
        if( createModuleRef ~= nil and createModuleRef[trans] ~= nil ) then
          G_Transform = createModuleRef[trans];
        end
      end
      -- Last we try the UdfFunctionTable, In case the user wants to employ
      -- one of the standard Functions.
      if( G_Transform == nil and functionTable ~= nil ) then
        G_Transform = functionTable[trans];
      end

      -- If we didn't find anything, BUT the user supplied a function name,
      -- then we have a problem.  We have to complain.
      if( G_Transform == nil ) then
        warn("[ERROR]<%s:%s> Transform Func not found: type(%s) Func(%s)",
          MOD, meth, type(trans), tostring(trans) );
        error( ldte.ERR_TRANS_FUN_NOT_FOUND );
      end
    end
  end

  GP=E and trace("[EXIT]<%s:%s>", MOD, meth );
end -- setWriteFunctions()

-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- createSearchPath: Create and initialize a search path structure so
-- that we can fill it in during our hash chain search.
-- Parms:
-- (*) ldtMap: topRec map that holds all of the control values
-- ======================================================================
local function createSearchPath( ldtMap )
  local sp        = map();
  sp.FoundLevel   = 0;      -- No valid level until we find something
  sp.LevelCount   = 0;      -- The number of levels we looked at.
  sp.RecList      = list(); -- Track all open nodes in the path
  sp.DigestList   = list(); -- The mechanism to open each level
  sp.PositionList = list(); -- Remember where the key was
  sp.HasRoom      = list(); -- Remember where there is room.

  return sp;
end -- createSearchPath()

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
    -- Set this control bin as HIDDEN
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM_LdtCount];
    recPropMap[RPM_LdtCount] = ldtCount + 1;
    GP=F and trace("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1 );
  end
  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  topRec[REC_LDT_CTRL_BIN] = recPropMap;

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
  return rc;
end -- setLdtRecordType()

-- ======================================================================
-- adjustLdtMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the ldtMap.
-- Parms:
-- (*) ldtMap: the main LSET Bin value
-- (*) argListMap: Map of LSET Settings 
-- ======================================================================
local function adjustLdtMap( ldtMap, argListMap )
  local meth = "adjustLdtMap()";
  GP=E and trace("[ENTER]: <%s:%s>:: LSetMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the create() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

  -- For the old style -- we'd iterate thru ALL arguments and change
  -- many settings.  Now we process only packages this way.
  for name, value in map.pairs( argListMap ) do
    GP=F and trace("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s)",
        MOD, meth, tostring( name ), tostring( value ));

    -- Process our "prepackaged" settings.  These now reside in the
    -- settings file.  All of the packages are in a table, and thus are
    -- looked up dynamically.
    -- Notice that this is the old way to change settings.  The new way is
    -- to use a "user module", which contains UDFs that control LDT settings.
    if name == "Package" and type( value ) == "string" then
      local ldtPackage = lsetPackage[value];
      if( ldtPackage ~= nil ) then
        ldtPackage( ldtMap );
      end
    end
  end -- for each argument

  GP=E and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));
      
  return ldtMap;
end -- adjustLdtMap

-- ======================================================================
-- local function ldtSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtCtrl )

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

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  resultMap.SUMMARY              = "LSET Summary";
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropSubRecCount      = propMap[PM_SubRecCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  resultMap.CreateTime           = propMap[PM_CreateTime];
  
    -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = ldtMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[M_LdrByteCountMax];
  
  -- General LSO Parms:
  resultMap.StoreMode            = ldtMap[M_StoreMode];
  resultMap.StoreState           = ldtMap[M_StoreState];
  resultMap.SetTypeStore         = ldtMap[M_SetTypeStore];
  resultMap.StoreLimit           = ldtMap[M_StoreLimit];
  resultMap.Transform            = ldtMap[M_Transform];
  resultMap.UnTransform          = ldtMap[M_UnTransform];
  resultMap.UserModule           = ldtMap[M_UserModule];
  resultMap.BinaryStoreSize      = ldtMap[M_BinaryStoreSize];
  resultMap.KeyType              = ldtMap[M_KeyType];
  resultMap.TotalCount			 = ldtMap[M_TotalCount];		
  resultMap.Modulo 				 = ldtMap[M_Modulo];
  resultMap.ThreshHold			 = ldtMap[M_ThreshHold];

  return resultMap;
end -- ldtSummary()

-- ======================================================================
-- local function ldtSummaryString( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
   GP=F and trace("Calling ldtSummaryString "); 
  return tostring( ldtSummary( ldtCtrl ));
end -- ldtSummaryString()

-- ======================================================================
-- initializeLdtCtrl:
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
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized ldtMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLdtCtrl(topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  GP=E and trace("[ENTER]: <%s:%s>::Bin(%s)",MOD, meth, tostring(ldtBinName));
  
  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific Lso Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local ldtMap = map();
  local ldtCtrl = list();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount] = 0; -- A count of all items in the stack
  propMap[PM_SubRecCount] = 0; -- A count of all Sub-Records in the LDT
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LSET; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = ldtBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]  = nil; -- not set yet.
  propMap[PM_CreateTime] = aerospike:get_current_time();

  -- Specific LSET Parms: Held in ldtMap
  ldtMap[M_StoreMode]   = SM_LIST; -- SM_LIST or SM_BINARY:
  ldtMap[M_StoreLimit]  = 0; -- No storage Limit

  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax]= 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[M_LdrByteEntrySize]=   0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax] =   0; -- Max # of Data Chunk Bytes (binary mode)

  ldtMap[M_Transform]        = nil; -- applies only to complex objects
  ldtMap[M_UnTransform]      = nil; -- applies only to complex objects
  ldtMap[M_StoreState]       = SS_COMPACT; -- SM_LIST or SM_BINARY:
  ldtMap[M_SetTypeStore]     = ST_RECORD; -- default is Top Record Store.
  ldtMap[M_HashType]         = HT_STATIC; -- Static or Dynamic
  ldtMap[M_BinaryStoreSize]  = nil; 
  -- Complex will work for both atomic/complex.
  ldtMap[M_KeyType]          = KT_COMPLEX; -- Most things will be complex
  ldtMap[M_TotalCount]       = 0; -- Count of both valid and deleted elements
  ldtMap[M_Modulo]           = DEFAULT_DISTRIB;
  ldtMap[M_ThreshHold]       = 101; -- Rehash after this many inserts
  ldtMap[M_BinListThreshold] = 4; -- Threshold for converting from a

  -- Put our new maps in a list, in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  topRec[ldtBinName]            = ldtCtrl;

  GP=F and trace("[DEBUG]: <%s:%s> : LSET Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  setLdtRecordType( topRec );

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtCtrl;

end -- initializeLdtCtrl()

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
  GP=E and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
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
  GP=E and trace("[ENTER]: <%s:%s> Bin(%d) ", MOD, meth, binNum );

  local binName = getBinName( binNum );
  -- create the first LSetBin_n LDT bin
  topRec[binName] = list(); -- Create a new list for this new bin

  -- This bin must now be considered HIDDEN:
  GP=E and trace("[DEBUG]: <%s:%s> Setting BinName(%s) as HIDDEN",
                 MOD, meth, binName );
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );

  GP=E and trace("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)",
                 MOD, meth, binNum, binName );

  return binName;
end -- setupNewBin

-- ======================================================================
-- Produce a COMPARABLE value (our overloaded term here is "key") from
-- the user's value.
-- The value is either simple (atomic) or an object (complex).  Complex
-- objects either have a key function defined, or we produce a comparable
-- "keyValue" from "value" by performing a tostring() operation.
--
-- NOTE: According to Chris (yes, everybody hates Chris), the tostring()
-- method will ALWAYS create the same string for complex objects that
-- have the same value.  We've noticed that tostring() does not always
-- show maps with fields in the same order, but in theory two objects (maps)
-- with the same content will have the same tostring() value.
-- Parms:
-- (*) ldtMap: The basic LDT Control structure
-- (*) value: The value from which we extract a compare-able "keyValue".
-- Return a comparable value:
-- ==> The original value, if it is an atomic type
-- ==> A Unique Identifier subset (that is atomic)
-- ==> The entire object, in string form.
-- ======================================================================
local function getKeyValue( ldtMap, value )
  local meth = "getKeyValue()";
  GP=E and trace("[ENTER]<%s:%s> value(%s) KeyType(%s)",
    MOD, meth, tostring(value), tostring(ldtMap[M_KeyType]) );

  if( value == nil ) then 
    GP=E and trace("[Early EXIT]<%s:%s> Value is nil", MOD, meth );
    return nil;
  end

  GP=E and trace("[DEBUG]<%s:%s> Value type(%s)", MOD, meth,
    tostring( type(value)));

  local keyValue;
  if( ldtMap[M_KeyType] == KT_ATOMIC or type(value) ~= "userdata" ) then
    keyValue = value;
  else
    if( G_KeyFunction ~= nil ) then
      -- Employ the user's supplied function (keyFunction).
      keyValue = G_KeyFunction( value );
    else
      -- If there's no shortcut, then take the "longcut" to get an atomic
      -- value that represents this entire object.
      keyValue = tostring( value );
    end
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
  return keyValue;
end -- getKeyValue();

-- ======================================================================
-- computeSetBin()
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single bin.
-- Second -- use the right hash function (depending on the type).
-- NOTE that we should be passed in ONLY KEYS, not objects, so we don't
-- need to do  "Key Extract" here, regardless of whether we're doing
-- ATOMIC or COMPLEX Object values.
-- ======================================================================
local function computeSetBin( key, ldtMap )
  local meth = "computeSetBin()";
  GP=E and trace("[ENTER]: <%s:%s> val(%s) Map(%s) ",
                 MOD, meth, tostring(key), tostring(ldtMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  -- Otherwise, Hash the key value, assuming it's either a number or a string.
  local binNumber  = 0; -- Default, if COMPACT mode
  if ldtMap[M_StoreState] == SS_REGULAR then
    -- There are really only TWO primitive types that we can handle,
    -- and that is NUMBER and STRING.  Anything else is just wrong!!
    if type(key) == "number" then
      binNumber  = numberHash( key, ldtMap[M_Modulo] );
    elseif type(key) == "string" then
      binNumber  = stringHash( key, ldtMap[M_Modulo] );
    else
      warn("[INTERNAL ERROR]<%s:%s>Hash(%s) requires type number or string!",
        MOD, meth, type(key) );
      error( ldte.ERR_INTERNAL );
    end
  end

  GP=E and trace("[EXIT]: <%s:%s> Key(%s) BinNumber (%d) ",
                 MOD, meth, tostring(key), binNumber );

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

-- =======================================================================
-- =======================================================================
-- =======================================================================

-- =============================
-- Begin SubRecord Function Area
-- =============================
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, like insert(), search(), remove() that use
-- Sub-Records will employ the "subrecContext" object, which will hold all
-- of the subrecords that were opened during processing.  Note that with
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
--
-- =======================================================================
-- searchList()
-- =======================================================================
-- Search a list for an item.  Each object (atomic or complex) is translated
-- into a "searchKey".  That can be a hash, a tostring or any other result
-- of a "uniqueIdentifier()" function.
--
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) binList: the list of values from the record
-- (*) searchKey: the "translated value"  we're searching for
-- Return the position if found, else return ZERO.
-- =======================================================================
local function searchList(ldtCtrl, binList, searchKey )
  local meth = "searchList()";
  GP=E and trace("[ENTER]: <%s:%s> Looking for searchKey(%s) in List(%s)",
     MOD, meth, tostring(searchKey), tostring(binList));
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local position = 0; 

  -- Nothing to search if the list is null or empty
  if( binList == nil or list.size( binList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyList", MOD, meth );
    return 0;
  end

  -- Search the list for the item (searchKey) return the position if found.
  -- Note that searchKey may be the entire object, or it may be a subset.
  local listSize = list.size(binList);
  local item;
  local dbKey;
  for i = 1, listSize, 1 do
    item = binList[i];
    GP=F and trace("[COMPARE]<%s:%s> index(%d) SV(%s) and ListVal(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(item));
    -- a value that does not exist, will have a nil binList item
    -- so we'll skip this if-loop for it completely                  
    if item ~= nil then
      if( G_UnTransform ~= nil ) then
        modValue = G_UnTransform( item );
      else
        modValue = item;
      end
      -- Get a "compare" version of the object.  This is either a summary
      -- piece, or just a "tostring()" of the entire object.
      dbKey = getKeyValue( ldtMap, modValue );
      GP=F and trace("[ACTUAL COMPARE]<%s:%s> index(%d) SV(%s) and dbKey(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(dbKey));
      if(dbKey ~= nil and type(searchKey) == type(dbKey) and searchKey == dbKey)
      then
        position = i;
        GP=F and trace("[FOUND!!]<%s:%s> index(%d) SV(%s) and dbKey(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(dbKey));
        break;
      end
    end -- end if not null and not empty
  end -- end for each item in the list

  GP=E and trace("[EXIT]<%s:%s> Result: Position(%d)", MOD, meth, position );
  return position;
end -- searchList()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result if they pass
-- the filter.
-- Parms:
-- (*) topRec:
-- (*) resultList: List holding search result
-- (*) ldtCtrl: The main LDT control structure
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function topRecScan( topRec, ldtCtrl, resultList )
  local meth = "topRecScan()";
  GP=E and trace("[ENTER]: <%s:%s> Scan all TopRec elements", MOD, meth );

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local listCount = 0;
  local liveObject = nil; -- the live object after "UnTransform"
  local resultFiltered = nil;

  -- Loop through all the modulo n lset-record bins 
  local distrib = ldtMap[M_Modulo];
  GP=F and trace(" Number of LSet bins to parse: %d ", distrib)
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    GP=F and trace(" Parsing through :%s ", tostring(binName))
	if topRec[binName] ~= nil then
      local objList = topRec[binName];
      if( objList ~= nil ) then
        for i = 1, list.size( objList ), 1 do
          if objList[i] ~= nil then
            if G_UnTransform ~= nil then
              liveObject = G_UnTransform( objList[i] );
            else
              liveObject = objList[i]; 
            end
            -- APPLY FILTER HERE, if we have one.
            if G_Filter ~= nil then
              resultFiltered = G_Filter( liveObject, G_FunctionArgs );
            else
              resultFiltered = liveObject;
            end
            list.append( resultList, resultFiltered );
          end -- end if not null and not empty
  		end -- end for each item in the list
      end -- if bin list not nil
    end -- end of topRec null check 
  end -- end for distrib list for-loop 

  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, list.size(resultList));

  return 0; 
end -- topRecScan


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result if they pass
-- the filter.
-- Parms:
-- (*) topRec:
-- (*) resultList: List holding search result
-- (*) ldtCtrl: The main LDT control structure
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function subRecScan( topRec, ldtCtrl, resultList )
  local meth = "subRecScan()";
  GP=E and trace("[ENTER]: <%s:%s> Scan all SubRec elements", MOD, meth );

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  warn("[ERROR]<%s:%s> Not Yet Implemented", MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, list.size(resultList));

  return 0; 
end -- subRecScan

-- ======================================================================
-- localTopRecInsert()
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtCtrl: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- RETURN:
--  0: ok
-- -1: Unique Value violation
-- ======================================================================
local function localTopRecInsert( topRec, ldtCtrl, newValue, stats )
  local meth = "localTopRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s>value(%s) stats(%s) ldtCtrl(%s)",
    MOD, meth, tostring(newValue), tostring(stats), ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1];  
  local ldtMap = ldtCtrl[2];
  local ldtBinName = propMap[PM_BinName];
  local rc = 0;
  
  -- We'll get the key and use that to feed to the hash function, which will
  -- tell us what bin we're in.
  local key = getKeyValue( ldtMap, newValue );
  local binNumber = computeSetBin( key, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  
  -- We're doing "Lazy Insert", so if a bin is not there, then we have not
  -- had any values for that bin (yet).  Allocate the list now.
  if binList == nil then
    GP=F and trace("[DEBUG]:<%s:%s> Creating List for binName(%s)",
                 MOD, meth, tostring( binName ) );
    binList = list();
  else
    -- Look for the value, and insert if it is not there.
    local position = searchList( ldtCtrl, binList, key );
    if( position > 0 ) then
      info("[ERROR]<%s:%s> Attempt to insert duplicate value(%s)",
        MOD, meth, tostring( newValue ));
      error(ldte.ERR_UNIQUE_KEY);
    end
  end
  -- If we have a transform, apply it now and store the transformed value.
  local storeValue = newValue;
  if( G_Transform ~= nil ) then
    storeValue = G_Transform( newValue );
  end
  list.append( binList, storeValue );

  topRec[binName] = binList; 
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

  -- Update stats if appropriate.
  if( stats == 1 ) then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = ldtMap[M_TotalCount];
    
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    ldtMap[M_TotalCount] = totalCount + 1; -- Total number of items goes up
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec,ldtBinName,BF_LDT_BIN);--Must set every time

    GP=F and trace("[STATUS]<%s:%s>Updating Stats TC(%d) IC(%d) Val(%s)",
      MOD, meth, ldtMap[M_TotalCount], propMap[PM_ItemCount], 
        tostring( newValue ));
  else
    GP=F and trace("[STATUS]<%s:%s>NOT updating stats(%d)",MOD,meth,stats);
  end

  GP=E and trace("[EXIT]<%s:%s>Insert Results: RC(%d) Value(%s) binList(%s)",
    MOD, meth, rc, tostring( newValue ), tostring(binList));

  return rc;
end -- localTopRecInsert()

-- ======================================================================
-- topRecRehashSet()
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) ldtCtrl
-- ======================================================================
local function topRecRehashSet( topRec, ldtCtrl )
  local meth = "topRecRehashSet()";
  GP=E and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1];  
  local ldtMap = ldtCtrl[2];

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
  ldtMap[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = ldtMap[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- assign a new list to topRec[binName]
    setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localTopRecInsert(topRec,ldtCtrl,listCopy[i],0); -- do NOT update counts.
  end

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- topRecRehashSet()

-- ======================================================================
-- initializeSubRec()
-- Set up a Hash Sub-Record 
-- There are potentially FOUR bins in a Sub-Record:
-- (0) nodeRec[SUBREC_PROP_BIN]: The Property Map
-- (1) nodeRec[LSR_CTRL_BIN]:   The control Map (defined here)
-- (2) nodeRec[LSR_LIST_BIN]:   The Data Entry List (when in list mode)
-- (3) nodeRec[LSR_BINARY_BIN]: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole LDT value is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 0,1,2 or Bins 0,1,3.
-- Parms:
-- (*) topRec
-- (*) ldtCtrl
-- (*) subRec
-- ======================================================================
local function initializeSubRec( topRec, ldtCtrl, subRec )
  local meth = "initializeSubRec()";
  GP=E and trace("[ENTER]:<%s:%s> ", MOD, meth );

  local topDigest = record.digest( topRec );
  local subRecDigest = record.digest( subRec );
  
  -- Extract the property map and control map from the ldt bin list.
  local topPropMap = ldtCtrl[1];
  local topLdtMap  = ldtCtrl[2];

  -- NOTE: Use Top level LSO entry for mode and max values
  --
  -- Set up the LDR Property Map
  subRecPropMap = map();
  subRecPropMap[PM_Magic] = MAGIC;
  subRecPropMap[PM_EsrDigest] = topPropMap[PM_EsrDigest]; 
  subRecPropMap[PM_RecType] = RT_SUB;
  subRecPropMap[PM_ParentDigest] = topDigest;
  subRecPropMap[PM_SelfDigest] = subRecDigest;
  -- For sub-recs, set create time to ZERO.
  subRecPropMap[PM_CreateTime] = 0;

  -- Set up the LDR Control Map
  subRecLdtMap = map();

  -- Depending on the StoreMode, we initialize the control map for either
  -- LIST MODE, or BINARY MODE
  if( topLdtMap[R_StoreMode] == SM_LIST ) then
    -- List Mode
    GP=F and trace("[DEBUG]: <%s:%s> Initialize in LIST mode", MOD, meth );
    subRecLdtMap[LF_ByteEntryCount] = 0;
    -- If we have an initial value, then enter that in our new object list.
    -- Otherwise, create an empty list.
    local objectList = list();
    if( firstValue ~= nil ) then
      list.append( objectList, firstValue );
      subRecLdtMap[LF_ListEntryCount] = 1;
      subRecLdtMap[LF_ListEntryTotal] = 1;
    else
      subRecLdtMap[LF_ListEntryCount] = 0;
      subRecLdtMap[LF_ListEntryTotal] = 0;
    end
    subRec[LSR_LIST_BIN] = objectList;
  else
    -- Binary Mode
    GP=F and trace("[DEBUG]: <%s:%s> Initialize in BINARY mode", MOD, meth );
    warn("[WARNING!!!]<%s:%s>Not ready for BINARY MODE YET!!!!", MOD, meth );
    subRecLdtMap[LF_ListEntryTotal] = 0;
    subRecLdtMap[LF_ListEntryCount] = 0;
    subRecLdtMap[LF_ByteEntryCount] = 0;
  end

  -- Take our new structures and put them in the subRec record.
  subRec[SUBREC_PROP_BIN] = subRecPropMap;
  subRec[LSR_CTRL_BIN] = subRecLdtMap;
  -- We must tell the system what type of record this is (sub-record)
  -- NOTE: No longer needed.  This is handled in the ldt setup.
  -- record.set_type( subRec, RT_SUB );

  aerospike:update_subrec( subRec );
  -- Note that the caller will write out the record, since there will
  -- possibly be more to do (like add data values to the object list).
  GP=F and trace("[DEBUG]<%s:%s> TopRec Digest(%s) subRec Digest(%s))",
    MOD, meth, tostring(topDigest), tostring(subRecDigest));

  GP=F and trace("[DEBUG]<%s:%s> subRecPropMap(%s) subRec Map(%s)",
    MOD, meth, tostring(subRecPropMap), tostring(subRecLdtMap));

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- initializeSubRec()

-- ======================================================================
-- Create a new subRec and initialize it.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) valueList: List of values (or nil)
-- NOTE: Remember that we must create an ESR when we create the first leaf
-- but that is the caller's job
-- Contents of a Leaf Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) LSR_CTRL_BIN:    Main Leaf Control structure
-- (3) LSR_LIST_BIN:    Object List goes here
-- (4) LSR_BINARY_BIN:  Packed Binary Array (if used) goes here
-- ======================================================================
local function createSubRec( src, topRec, ldtCtrl, valueList )
  local meth = "createSubRec()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Remember to add this to the SRC
  local subRec = aerospike:create_subrec( topRec );
  if( subRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating Subrec", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  -- Increase the Subrec Count
  local subrecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subrecCount + 1;

  local rc = initializeSubRec( topRec, ldtCtrl, subRec, firstValue );
  if( rc >= 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>Leaf Init OK", MOD, meth );
    rc = aerospike:update_subrec( subRec );
  else
    warn("[ERROR]<%s:%s> Problems initializing Leaf(%d)", MOD, meth, rc );
    error( ldte.ERR_INTERNAL );
  end

  -- Must wait until subRec is initialized before it can be added to SRC.
  -- It should be ready now.
  addSubrecToContext( src, subRec );

  GP=F and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return subRec;
end -- createSubRec()

-- ======================================================================
-- subRecSearch()
-- ======================================================================
-- Search this chain of subrecs (should be only one, but might be more)
-- Parms:
-- (*) src:  SubRec Context
-- (*) topRec: 
-- (*) digestString: Digest of the head of the chain
-- (*) key: The value (or subvalue) that we're searching for.
-- Return: 
-- Successful operation:
-- ==> Found:  {position, subRecPtr}
-- ==> NOT Found:  {0, subRecPtr}
-- Extreme Error -- longjump out
-- ======================================================================
local function subRecSearch( src, topRec, ldtCtrl, digest, key )
  local meth = "subRecSearch()";
  
  GP=E and trace("[ENTER]:<%s:%s>Digest(%s) SearchVal(%s)",
    MOD, meth, digestString, tostring(key));
    
-- local X_ItemCount              = 'I'; -- Number of items for this dir cell
-- local X_SubRecordCount         = 'S'; -- Number of sub recs for this dir cell
-- local X_DigestHead             = 'D'; -- Head of the Sub-Rec list
-- local X_ListHead               = 'L'; -- Direct list (if not a Sub-Rec list)

  local digestString;
  local subRec;
  local subRecList;
  local nextDigest = digest;
  while nextDigest ~= nil do
    digestString = tostring( nextDigest );
    subRec = openSubrec( src, topRec, digestString );
    subRecList = subRec[LSR_LIST_BIN];

    position = searchList( ldtCtrl, subRecList, key )

    -- If we found it, return the position and subrec.
    --
    -- If we did NOT find it, and there's room, return ZERO + subrec




  end -- end while

  -- TODO: Finish
  --
  --
  warn("[ERROR]<%s:%s> FUNCTION NOT YET IMPLEMENTED", MOD, meth );
  if( cellAnchor[X_ListHead] ~= nil ) then
    -- Search the basic list
    binList = cellAnchor[X_ListHead];
    position = searchList( ldtCtrl, binList, key );
    if( position == 0 ) then
      list.append( binList, newValue );
    else
      error(ldte.ERR_UNIQUE_KEY);
    end
    -- If this initial list is over the threshold, then move this list
    -- into a subrec.
    local listSize = list.size( binList );
    if( listSize > ldtMap[M_BinListThreshold] ) then
      -- Ok -- create the subrec and attach the list to it.
      subrec = createSubRec( src, topRec, ldtCtrl, binList );
      cellAnchor[X_DigestHead] = subrec;
    end
  elseif( cellAnchor[X_DigestHead] ~= nil ) then
    local digestString = tostring( cellAnchor[X_DigestHead] );
    subrec = openSubrec( src, topRec, digestString )
    position, subrec = subRecSearch( src, topRec, digestString, key );
  end


  local position = 0;
  local subRec = 0;
  -- while digestString= openSubrec( src, topRec, digestString );

  return position, subRec;
end -- subRecSearch()

-- ======================================================================
-- topRecSearch
-- ======================================================================
-- In Top Record Mode,
-- Find an element (i.e. search), and optionally apply a filter.
-- Return the element if found, return an error (NOT FOUND) otherwise
-- Parms:
-- (*) topRec: Top Record -- needed to access numbered bins
-- (*) ldtCtrl: the main LDT Control structure
-- (*) searchKey: This is the value we're looking for.
-- NOTE: We've now switched to using a different mode for the filter
-- and the "UnTransform" function.  We set that up once on entry into
-- the search function (using the new lookup mechanism involving user
-- modules).
-- Return: The Found Object, or Error (not found)
-- ======================================================================
local function topRecSearch( topRec, ldtCtrl, searchKey )
  local meth = "topRecSearch()";
  GP=E and trace("[ENTER]: <%s:%s> Search Key(%s)",
                 MOD, meth, tostring( searchKey ) );

  local rc = 0; -- Start out ok.

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Find the appropriate bin for the Search value
  local binNumber = computeSetBin( searchKey, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local liveObject = nil;
  local resultFitlered = nil;
  local position = 0;

  GP=F and trace("[DEBUG]<%s:%s> UnTrans(%s) Filter(%s) SrchKey(%s) List(%s)",
    MOD, meth, tostring(G_UnTransform), tostring( G_Filter),
    tostring(searchKey), tostring(binList));

  -- We bother to search only if there's a real list.
  if binList ~= nil and list.size( binList ) > 0 then
    position = searchList( ldtCtrl, binList, searchKey );
    if( position > 0 ) then
      -- Apply the filter to see if this item qualifies
      -- First -- we have to untransform it (sadly, again)
      local item = binList[position];
      if G_UnTransform ~= nil then
        liveObject = G_UnTransform( item );
      else
        liveObject = item;
      end

      -- APPLY FILTER HERE, if we have one.
      if G_Filter ~= nil then
        resultFiltered = G_Filter( liveObject, G_FunctionArgs );
      else
        resultFiltered = liveObject;
      end
    end -- if search found something (pos > 0)
  end -- if there's a list

  if( resultFiltered == nil ) then
    warn("[WARNING]<%s:%s> Value not found: Value(%s)",
      MOD, meth, tostring( searchKey ) );
    error( ldte.ERR_NOT_FOUND );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Success: SearchKey(%s) Result(%s)",
     MOD, meth, tostring(searchKey), tostring( resultFiltered ));
  return resultFiltered;
end -- function topRecSearch()


-- ======================================================================
-- localSubRecInsert()
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtCtrl: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- RETURN:
--  0: ok
-- -1: Unique Value violation
-- ======================================================================
local function localSubRecInsert( src, topRec, ldtCtrl, newValue, stats )
  local meth = "localSubRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s>Insert(%s) stats(%s)",
    MOD, meth, tostring(newValue), tostring(stats));

  local propMap = ldtCtrl[1];  
  local ldtMap = ldtCtrl[2];
  local ldtBinName = propMap[PM_BinName];
  local rc = 0;
  
  -- We'll get the key and use that to feed to the hash function, which will
  -- tell us what bin we're in.
  local key = getKeyValue( ldtMap, newValue );
  local binCell = computeSetBin( key, ldtMap );
  local hashDirectory = ldtMap[M_HashDirectory];
  local cellAnchor = hashDirectory[binCell];
  -- If there's a ListHead, then that means we don't have a Sub-Record stored
  -- here yet.  We'll keep up to "BinListThreshold" items
  local binList;
  local subrec;
  local headSubrec;
  local position;
  if( cellAnchor[X_ListHead] ~= nil ) then
    -- Search the basic list
    binList = cellAnchor[X_ListHead];
    position = searchList( ldtCtrl, binList, key );
    if( position == 0 ) then
      list.append( binList, newValue );
    else
      error(ldte.ERR_UNIQUE_KEY);
    end
    -- If this initial list is over the threshold, then move this list
    -- into a subrec.
    local listSize = list.size( binList );
    if( listSize > ldtMap[M_BinListThreshold] ) then
      -- Ok -- create the subrec and attach the list to it.
      subrec = createSubRec( src, topRec, ldtCtrl, binList );
      cellAnchor[X_DigestHead] = subrec;
    end
  elseif( cellAnchor[X_DigestHead] ~= nil ) then
    local digestString = tostring( cellAnchor[X_DigestHead] );
    -- subrec = openSubrec( src, topRec, digestString )
    position, subrec = subRecSearch( src, topRec, digestString, key );

    warn("ERROR!! <%s:%s> NOT YET FINISHED!!!", MOD, meth );
    warn("ERROR!! <%s:%s> NOT YET FINISHED!!!", MOD, meth );
    warn("ERROR!! <%s:%s> NOT YET FINISHED!!!", MOD, meth );
    warn("ERROR!! <%s:%s> NOT YET FINISHED!!!", MOD, meth );

-- local LDR_ByteEntryCount       = 'C'; -- Current Count of bytes used
-- local LDR_NextSubRec           = 'N'; -- Next Subrec in the chain
    
  else
    -- Nothing here -- create a new list, add to it, save it.
    binList = list();
    list.append( binList, newValue );
    cellAnchor[X_ListHead] = binList;
  end

  
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local insertResult = 0;
  
  -- We're doing "Lazy Insert", so if a bin is not there, then we have not
  -- had any values for that bin (yet).  Allocate the list now.
  if binList == nil then
    GP=F and trace("[DEBUG]:<%s:%s> Creating List for binName(%s)",
                 MOD, meth, tostring( binName ) );
    binList = list();
  end
  -- Look for the value, and insert if it is not there.
  local position = searchList( ldtCtrl, binList, key );
  if( position == 0 ) then
    list.append( binList, newValue );
    insertResult = 1;
    topRec[binName] = binList; 
    record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

    -- Update stats if appropriate.
    if( stats == 1 ) then -- Update Stats if success
      local itemCount = propMap[PM_ItemCount];
      local totalCount = ldtMap[M_TotalCount];
    
      propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
      ldtMap[M_TotalCount] = totalCount + 1; -- Total number of items goes up
      topRec[ldtBinName] = ldtCtrl;
      record.set_flags(topRec,ldtBinName,BF_LDT_BIN);--Must set every time

      GP=F and trace("[STATUS]<%s:%s>Updating Stats TC(%d) IC(%d)", MOD, meth,
        ldtMap[M_TotalCount], propMap[PM_ItemCount] );
    else
      GP=F and trace("[STATUS]<%s:%s>NOT updating stats(%d)",MOD,meth,stats);
    end
  else
    rc = -1;
    warn("[UNIQUENESS VIOLATION]<%s:%s> Attempt to insert duplicate value(%s)",
      MOD, meth, tostring( newValue ));
    error(ldte.ERR_UNIQUE_KEY);
  end

  GP=E and trace("[EXIT]<%s:%s>Insert Results: RC(%d) Value(%s) binList(%s)",
    MOD, meth, rc, tostring( newValue ), tostring(binList));

  return rc;
end -- localSubRecInsert

-- ======================================================================
-- subRecRehashSet()
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from the compact list, null out the
-- compact list and then reinsert them into the regular hash directory.
-- Parms:
-- (*) topRec
-- (*) ldtCtrl
-- ======================================================================
local function subRecRehashSet( topRec, ldtCtrl )
  local meth = "subRecRehashSet()";
  GP=E and trace("[ENTER]:<%s:%s> !!!! SUBREC REHASH !!!! ", MOD, meth );
  GP=E and trace("[ENTER]:<%s:%s> !!!! SUBREC REHASH !!!! ", MOD, meth );

  local propMap = ldtCtrl[1];  
  local ldtMap = ldtCtrl[2];

  local compactList = ldtMap[M_CompactList];
  if compactList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty list", MOD, meth );
    error( ldte.ERR_INSERT );
  end

  ldtMap[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- Rebuild. Insert into the hash diretory.
  --
  -- Create the subrecs as needed.  But, allocate a hash cell anchor structure
  -- for each directory entry.
  --
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = ldtMap[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- assign a new list to topRec[binName]
    setupNewCell( topRec, i );
  end -- for each new hash cell

  for i = 1, list.size(listCopy), 1 do
    localsubRecInsert( src, topRec, ldtCtrl, listCopy[i], 0 ); -- no count update
  end

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- subRecRehashSet()

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
-- Check that the topRec, the ldtBinName and ldtMap are valid, otherwise
-- jump out with an error() call.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName: User's Name -- not currently used
-- ======================================================================
local function validateRecBinAndMap( topRec, ldtBinName, mustExist )
  local meth = "validateRecBinAndMap()";

  GP=E and trace("[ENTER]: <%s:%s>  ", MOD, meth );

  -- Validate that the user's supplied BinName will work:
  -- ==========================================================
  -- Now that we have changed to using the user's name, we need to validate
  -- the user's bin name.
  validateBinName( ldtBinName );

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
      
    -- Control Bin Must Exist, in this case, ldtCtrl is what we check
    if( topRec[ldtBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSET_BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    local ldtCtrl = topRec[ldtBinName]; -- The main lset map
    local propMap = ldtCtrl[1];
    local ldtMap  = ldtCtrl[2];
    
    if(propMap[PM_Magic] ~= MAGIC) or propMap[PM_LdtType] ~= LDT_TYPE_LSET then
      GP=E and warn("[ERROR EXIT]:<%s:%s>LSET_BIN(%s) Corrupted:No magic:1",
            MOD, meth, ldtBinName );
      error( ldte.ERR_BIN_DAMAGED );
    end
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[ldtBinName] ~= nil then
       local ldtCtrl = topRec[ldtBinName]; -- The main lset map
       local propMap = ldtCtrl[1];
       local ldtMap  = ldtCtrl[2];
    
       if( propMap[PM_Magic] ~= MAGIC ) or propMap[PM_LdtType] ~= LDT_TYPE_LSET
         then
        GP=E and warn("[ERROR EXIT]:<%s:%s>LSET_BIN<%s:%s>Corrupted:No magic:2",
              MOD, meth, ldtBinName, tostring( ldtMap ));
        error( ldte.ERR_BIN_DAMAGED );
      end
    end
  end
end -- validateRecBinAndMap()


-- ======================================================================
-- processModule()
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

  if( moduleName ~= nil ) then
    if( type(moduleName) ~= "string" ) then
      warn("[ERROR]<%s:%s>User Module(%s) not valid::wrong type(%s)",
        MOD, meth, tostring(moduleName), type(moduleName));
      error( ldte.ERR_USER_MODULE_BAD );
    end

    local userModule = require(moduleName);
    if( userModule == nil ) then
      warn("[ERROR]<%s:%s>User Module(%s) not valid", MOD, meth, moduleName);
      error( ldte.ERR_USER_MODULE_NOT_FOUND );
    else
      local userSettings =  userModule[G_SETTINGS];
      if( userSettings ~= nil ) then
        userSettings( ldtMap ); -- hope for the best.
        ldtMap[M_UserModule] = moduleName;
      end
    end
  else
    warn("[ERROR]<%s:%s>User Module is NIL", MOD, meth );
  end

  GP=E and trace("[EXIT]<%s:%s> Module(%s) LDT CTRL(%s)", MOD, meth,
    tostring( moduleName ), ldtSummaryString(ldtCtrl));

end -- processModule()


-- ======================================================================
-- setupLdtBin()
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, userModule ) 
  local meth = "setupLdtBin()";
  GP=E and trace("[ENTER]<%s:%s> binName(%s)",MOD,meth,tostring(ldtBinName));

  local ldtCtrl = initializeLdtCtrl( topRec, ldtBinName );
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  
  -- Set the type of this record to LDT (it might already be set)
  -- No Longer needed.  The Set Type is handled in initializeLdtCtrl()
  -- record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in settings that override the defaults
  -- (the userModule), then process that now.
  if( userModule ~= nil )then
    local createSpecType = type(userModule);
    if( createSpecType == "string" ) then
      processModule( ldtCtrl, userModule );
    elseif( createSpecType == "userdata" ) then
      adjustLdtMap( ldtMap, userModule );
    else
      warn("[WARNING]<%s:%s> Unknown Creation Object(%s)",
        MOD, meth, tostring( userModule ));
    end
  end

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
                 MOD, meth , tostring(ldtMap));

  -- Sets the topRec control bin attribute to point to the 2 item list
  -- we created from InitializeLSetMap() : 
  -- Item 1 :  the property map & Item 2 : the ldtMap
  
  topRec[ldtBinName] = ldtCtrl; -- store in the record

  -- initializeLdtCtrl always sets ldtMap[M_StoreState] to SS_COMPACT
  -- At this point there is only one bin.
  -- This one will assign the actual record-list to topRec[binName]
  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Setup the compact list in sub-rec mode
    ldtMap[M_CompactList] = list();
  else
    -- Setup the compact list in sub-rec mode
    setupNewBin( topRec, 0 );
  end

  -- NOTE: The Caller will write out the LDT bin.
  return 0;
end -- setupLdtBin()


-- ======================================================================
-- || localLSetCreate ||
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
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) userModule: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
--
local function localLSetCreate( topRec, ldtBinName, userModule )
  local meth = "localLSetCreate()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring(userModule) );

  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if ldtBinName is bad.
  validateBinName( ldtBinName );

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LDT field to an
  -- existing record.
  if( topRec[ldtBinName] ~= nil ) then
    warn("[ERROR EXIT]: <%s:%s> LDT BIN (%s) Already Exists",
                   MOD, meth, ldtBinName );
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.
  
  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );

  -- We need a new LDT bin -- set it up.
  setupLdtBin( topRec, ldtBinName, userModule );

  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  if( rc == nil or rc == 0 ) then
    return 0;
  else
    error( ldte.ERR_CREATE );
  end
end -- localLSetCreate()

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
-- (*) ldtCtrl: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- ======================================================================
local function topRecInsert( topRec, ldtCtrl, newValue )
  local meth = "topRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s) NewValue(%s)",
                 MOD, meth, tostring(ldtCtrl), tostring( newValue ) );

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local totalCount = ldtMap[M_TotalCount];
  local itemCount = propMap[PM_ItemCount];
  
  GP=F and trace("[DEBUG]<%s:%s>Store State(%s) Total Count(%d) ItemCount(%d)",
    MOD, meth, tostring(ldtMap[M_StoreState]), totalCount, itemCount );

  if ldtMap[M_StoreState] == SS_COMPACT and
    totalCount >= ldtMap[M_ThreshHold]
  then
    GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH BEFORE INSERT", MOD, meth);
    topRecRehashSet( topRec, ldtCtrl );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  -- localTopRecInsert() will jump out with its own error call if something bad
  -- happens so no return code (or checking) needed here.
  localTopRecInsert( topRec, ldtCtrl, newValue, 1 );

  -- NOTE: the update of the TOP RECORD has already
  -- been taken care of in localTopRecInsert, so we don't need to do it here.
  --
  -- All done, store the record
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  if( rc == nil or rc == 0 ) then
      return 0;
  else
      error( ldte.ERR_INSERT );
  end
end -- function topRecInsert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || SubRec Insert
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Insert a value into the set, using the SubRec design.
-- Take the value, perform a hash and a modulo function to determine which
-- directory cell is used, open the appropriate subRec, then add to the list.
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtCtrl: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- ======================================================================
local function subRecInsert( topRec, ldtCtrl, newValue )
  local meth = "subRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s) NewValue(%s)",
                 MOD, meth, ldtSummaryString(ldtCtrl), tostring( newValue ));

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local src = createSubrecContext();
  local ldtBinName = propMap[PM_BinName];

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash the compact list into the full directory structure.
  local totalCount = ldtMap[M_TotalCount];
  local itemCount = propMap[PM_ItemCount];
  
  GP=F and trace("[DEBUG]<%s:%s>Store State(%s) Total Count(%d) ItemCount(%d)",
    MOD, meth, tostring(ldtMap[M_StoreState]), totalCount, itemCount );

  if ldtMap[M_StoreState] == SS_COMPACT and
    totalCount >= ldtMap[M_ThreshHold]
  then
    GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH BEFORE INSERT", MOD, meth);
    subRecRehashSet( topRec, ldtBinName, ldtCtrl );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  -- localSubRecInsert() will jump out with its own error call if something bad
  -- happens so no return code (or checking) needed here.
  localSubRecInsert( src, topRec, ldtCtrl, newValue, 1 );

  -- NOTE: the update of the TOP RECORD has already
  -- been taken care of in localSubRecInsert, so we don't need to do it here.
  --
  -- Store it again here -- for now.  Remove later, when we're sure.  
  topRec[ldtBinName] = ldtCtrl;
  -- Also -- in Lua -- all data (like the maps and lists) are inked by
  -- reference -- so they do not need to be "re-updated".  However, the
  -- record itself, must have the object re-assigned to the BIN.
  -- Also -- must ALWAYS reset the bin flag, every time.
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  if( rc == nil or rc == 0 ) then
      return 0;
  else
      error( ldte.ERR_INSERT );
  end
end -- function subRecInsert()

-- ======================================================================
-- localLSetInsert()
-- ======================================================================
-- Perform the local insert, for whichever mode (toprec, subrec) is
-- called for.
-- ======================================================================
local function localLSetInsert( topRec, ldtBinName, newValue, userModule )
  local meth = "localLSetInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s> LSetBin(%s) NewValue(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring( newValue ),
                 tostring( userModule ));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- If the bin is not already set up, then create.
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO]: <%s:%s> LSET BIN (%s) does not Exist:Creating",
         MOD, meth, tostring( ldtBinName ));

    -- We need a new LDT bin -- set it up.
    setupLdtBin( topRec, ldtBinName, userModule );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lset control structure
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Set up the Read/Write Functions (KeyFunction, Transform, Untransform)
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, nil, nil, nil );
  setWriteFunctions( ldtMap );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Insert
    subRecInsert( topRec, ldtCtrl, newValue );
  else
    -- Use the TopRec style Insert (this is default if the subrec style
    -- is not specifically requested).
    topRecInsert( topRec, ldtCtrl, newValue );
  end

  -- Update the Count, then update the Record.
  -- local itemCount = propMap[PM_ItemCount];
  -- propMap[PM_ItemCount] = itemCount + 1;
  -- topRec[ldtBinName] = ldtCtrl;
  -- record.set_flags(topRec, ldtBinName, BF_LDT_HIDDEN );--Must set every time

  rc = aerospike:update( topRec );
  if( rc ~= nil and rc ~= 0 ) then
    warn("[WARNING]:<%s:%s> Bad Update Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]:<%s:%s> RC(0)", MOD, meth );
  return 0;
end -- localLSetInsert()

-- ======================================================================
-- localLSetInsertAll() -- with and without create
-- ======================================================================
-- ======================================================================
local function localLSetInsertAll( topRec, ldtBinName, valueList, userModule )
  local meth = "lset_insert_all()";
  local rc = 0;
  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      rc = localLSetInsert( topRec, ldtBinName, valueList[i], userModule );
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
end -- localLSetInsertAll()

-- ======================================================================
-- ======================================================================
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Large Set Exists
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return 1 item if the item exists in the set, otherwise return 0.
-- We don't want to return "true" and "false" because of Lua Weirdness.
-- Note that this looks a LOT like localLSetSearch(), except that we don't
-- return the object, nor do we apply a filter.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- ======================================================================
local function localLSetExists( topRec, ldtBinName, searchValue )

  local meth = "localLSetExists()";
  GP=E and trace("[ENTER]: <%s:%s> Search Value(%s)",
                 MOD, meth, tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject = 0;

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local key = getKeyValue( ldtMap, searchValue );

  -- Set up our global "UnTransform" and Filter Functions. This lets us
  -- process the function pointers once per call, and consistently for
  -- all LSET operations. (However, filter not used here.)
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, nil, nil, nil );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Search
    resultObject = subRecSearch( topRec, ldtCtrl, key );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, key );
  end

  local result = 1; -- be positive.
  if( resultObject == nil ) then
    result = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s>: Exists Result(%d)",MOD, meth, result ); 
  return result;
end -- function localLSetExist()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || localLSetSearch
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search), and optionally apply a filter.
-- Return the element if found, return an error (NOT FOUND) otherwise
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Optional Arguments to feed to the filter
-- Return the object, or Error (NOT FOUND)
-- ======================================================================
local function localLSetSearch( topRec, ldtBinName, searchValue,
        userModule, filter, fargs)

  local meth = "localLSetSearch()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) Search Value(%s)",
     MOD, meth, tostring( ldtBinName), tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject = 0;

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local key = getKeyValue( ldtMap, searchValue );

  -- Set up our global "UnTransform" and Filter Functions.  This lets us
  -- process the function pointers once per call, and consistently for
  -- all LSET operations.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Search
    resultObject = subRecSearch( topRec, ldtCtrl, key );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, key );
  end

  -- Report an error if we did not find the object.
  if( resultObject == nil ) then
    info("[NOT FOUND]<%s:%s> SearchValue(%s)",MOD,meth,tostring(searchValue));
    error(ldte.ERR_NOT_FOUND);
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)",MOD,meth,tostring(resultObject));
  return resultObject;
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
local function localLSetScan(topRec, ldtBinName, userModule, filter, fargs)
  local meth = "localLSetScan()";

  rc = 0; -- start out OK.
  GP=E and trace("[ENTER]<%s:%s> BinName(%s) Module(%s) Filter(%s) Fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(userModule), tostring(filter),
    tostring(fargs));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Find the appropriate bin for the Search value
  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultList = list();
  --
  -- Set up our global "UnTransform" and Filter Functions.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style scan
    subRecScan( topRec, ldtCtrl, resultList );
  else
    -- Use the TopRec style Scan (this is default Mode if the subrec style
    -- is not specifically requested).
    topRecScan( topRec, ldtCtrl, resultList );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s) Size : %d",
                 MOD, meth, tostring(resultList), list.size(resultList));

  return resultList; 
end -- function localLSetScan()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || topRecDelete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Top Record Mode
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtCtrl
-- (*) deleteValue:
-- (*) returnVal;  when true, return the deleted value.
-- ======================================================================
local function topRecDelete( topRec, ldtCtrl, deleteValue, returnVal)
  local meth = "topRecDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  -- Get the value we'll compare against
  local key = getKeyValue( ldtMap, deleteValue );

  -- Find the appropriate bin for the Search value
  local binNumber = computeSetBin( key, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local liveObject = nil;
  local resultFitlered = nil;
  local position = 0;

  -- We bother to search only if there's a real list.
  if binList ~= nil and list.size( binList ) > 0 then
    position = searchList( ldtCtrl, binList, key );
    if( position > 0 ) then
      -- Apply the filter to see if this item qualifies
      -- First -- we have to untransform it (sadly, again)
      local item = binList[position];
      if G_UnTransform ~= nil then
        liveObject = G_UnTransform( item );
      else
        liveObject = item;
      end

      -- APPLY FILTER HERE, if we have one.
      if filterFunction ~= nil then
        resultFiltered = filterFunction( liveObject, fargs );
      else
        resultFiltered = liveObject;
      end
    end -- if search found something (pos > 0)
  end -- if there's a list

  if( position == 0 or resultFiltered == nil ) then
    warn("[WARNING]<%s:%s> Value not found: Value(%s) SearchKey(%s)",
      MOD, meth, tostring(deleteValue), tostring(key));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok, we got the value.  Remove it and update the record.  Also,
  -- update the stats.
  -- OK -- we can't simply NULL out the entry -- because that breaks the bin
  -- when we try to store.  So -- we'll instead replace this current entry
  -- with the END entry -- and then we'll COPY (take) the list ... until
  -- we have the ability to truncate a list in place.
  local listSize = list.size( binList );
  if( position < listSize ) then
    binList[position] = binList[listSize];
  end
  local newBinList = list.take( binList, listSize - 1 );

  topRec[binName] = newBinList;
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

  -- The caller will update the stats and update the main ldt bin.

  GP=E and trace("[EXIT]<%s:%s>: Success: DeleteValue(%s) Res(%s) binList(%s)",
    MOD, meth, tostring( deleteValue ), tostring(resultFiltered),
    tostring(binList));
  if( returnVal == true ) then
    return resultFiltered;
  else
    return 0;
  end
end -- function topRecDelete()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) deleteValue:
-- (*) UserModule
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Arguments to feed to the filter
-- (*) returnVal: When true, return the deleted value.
-- ======================================================================
local function localLSetDelete( topRec, ldtBinName, deleteValue, userModule,
                                filter, fargs, returnVal)

  local meth = "localLSetDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[ldtBinName] == nil ) then
    GP=E and trace("[ERROR EXIT]: <%s:%s> LSetCtrlBin does not Exist",
                   MOD, meth );
    error( ldte.ERR_BIN_DOES_NOT_EXIST );
  end

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject;

  -- Get the value we'll compare against
  local key = getKeyValue( ldtMap, deleteValue );

  -- Set up our global "UnTransform" and Filter Functions.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style delete
    resultObject = subRecDelete( topRec, ldtCtrl, key, returnVal);
  else
    -- Use the TopRec style delete
    resultObject = topRecDelete( topRec, ldtCtrl, key, returnVal );
  end

  -- Update the Count, then update the Record.
  local itemCount = propMap[PM_ItemCount];
  propMap[PM_ItemCount] = itemCount - 1;
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  rc = aerospike:update( topRec );
  if( rc ~= nil and rc ~= 0 ) then
    warn("[WARNING]:<%s:%s> Bad Update Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]<%s:%s>: Success: DeleteValue(%s) Res(%s) binList(%s)",
    MOD, meth, tostring( deleteValue ), tostring(resultFiltered),
    tostring(binList));
  if( returnVal == true ) then
    return resultObject;
  else
    return 0;
  end
end -- function localLSetDelete()

-- ========================================================================
-- localGetSize() -- return the number of elements (item count) in the set.
-- ========================================================================
local function localGetSize( topRec, ldtBinName )
  local meth = "lset_size()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lset control structure
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  local itemCount = propMap[PM_ItemCount];

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function localGetSize()

-- ========================================================================
-- localGetConfig() -- return the config settings
-- ========================================================================
local function localGetConfig( topRec, ldtBinName )
  local meth = "localGetConfig()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
      MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local config = ldtSummary( topRec[ ldtBinName ] );

  GP=E and trace("[EXIT]:<%s:%s>:config(%s)", MOD, meth, tostring(config));

  return config;
end -- function localGetConfig()

-- ========================================================================
-- subRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function subRecDump( topRec, ldtCtrl )
  local meth = "subRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Loop through the Hash Directory
  warn("ERROR:: SUBREC DUMP NOT YET IMPLEMENTED!!!");

  GP=E and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

end -- subRecDump();


-- ========================================================================
-- topRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function topRecDump( topRec, ldtCtrl )
  local meth = "subRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Loop through all the modulo n lset-record bins 
  local distrib = ldtMap[M_Modulo];

  GP=F and trace(" Number of LSet bins to parse: %d ", distrib)

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
    trace("[DEBUG]<%s:%s> BIN(%s) TList(%s) B List(%s)", MOD, meth, binName,
      tostring(tempList), tostring(binList));
    list.append( resultList, binList );
  end -- end for distrib list for-loop 

  GP=E and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

end -- topRecDump();


-- ========================================================================
-- localDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function localDump( topRec, ldtBinName )
  local meth = "localDump()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)", MOD, meth,tostring(ldtBinName));

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  -- Check once for the untransform functions -- so we don't need
  -- to do it inside the loop.  No filters here, though.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, nil, nil, nil );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy
    subRecDump( topRec, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    topRecDump( topRec, ldtCtrl );
  end

  local ret = " \n LSet bin contents dumped to server-logs \n"; 
  return ret; 
end -- localDump();

-- ========================================================================
-- topRecDestroy() -- Remove the LDT entirely from the TopRec LSET record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function topRecDestroy( topRec, ldtCtrl )
  local meth = "topRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));
  local rc = 0; -- start off optimistic

  -- The caller has already dealt with the Common/Hidden LDT Prop Bin.
  -- All we need to do here is deal with the Numbered bins.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local ldtBinName = propMap[PM_BinName];

  -- Address the TopRecord version here.
  -- Loop through all the modulo n lset-record bins 
  -- Go thru and remove (mark nil) all of the LSET LIST bins.
  local distrib = ldtMap[M_Modulo];
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    -- Remove this bin -- assuming it is not already nil.  Setting a 
    -- non-existent bin to nil seems to piss off the lower layers. 
    if( topRec[binName] ~= nil ) then
        topRec[binName] = nil;
    end
  end -- end for distrib list for-loop 

  -- Mark the enitre control-info structure nil.
  topRec[ldtBinName] = nil;

end -- topRecDestroy()


-- ========================================================================
-- subRecDestroy() -- Remove the LDT (and subrecs) entirely from the record.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function subRecDestroy( topRec, ldtCtrl )
  local meth = "subRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));
  local rc = 0; -- start off optimistic

  -- Extract the property map and lso control map from the LDT Control
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), ldtSummaryString( ldtCtrl ));

  -- Get the ESR and delete it -- if it exists.  If we have ONLY an initial
  -- compact list, then the ESR will be ZERO.
  local esrDigest = propMap[PM_EsrDigest];
  if( esrDigest ~= nil and esrDigest ~= 0 ) then
    local esrDigestString = tostring(esrDigest);
    info("[SUBREC OPEN]<%s:%s> Digest(%s)", MOD, meth, esrDigestString );
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

  topRec[binName] = nil;

end -- subRecDestroy()


-- ========================================================================
-- localLdtDestroy() -- Remove the LDT entirely from the record.
-- NOTE: This could eventually be moved to COMMON, and be "localLdtDestroy()",
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
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function localLdtDestroy( topRec, ldtBinName )
  local meth = "localLdtDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and lso control map from the lso bin list.

  local ldtCtrl = topRec[ldtBinName]; -- The main lset map
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
      MOD, meth );
    error( ldte.ERR_INTERNAL );
  end
  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- This is the last LDT -- remove the LDT Control Property Bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  end

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy
    resultObject = subRecDestroy( topRec, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecDestroy( topRec, ldtCtrl );
  end

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
end -- localLdtDestroy()

-- ========================================================================
-- localSetCapacity() -- set the current capacity setting for this LDT
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
local function localSetCapacity( topRec, ldtBinName, capacity )
  local meth = "localSetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local ldtMap = ldtCtrl[2];
  if( capacity ~= nil and type(capacity) == "number" and capacity >= 0 ) then
    ldtMap[M_StoreLimit] = capacity;
  else
    warn("[ERROR]<%s:%s> Bad Capacity Value(%s)",MOD,meth,tostring(capacity));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]: <%s:%s> : new size(%d)", MOD, meth, capacity );

  return 0;
end -- function localSetCapacity()

-- ========================================================================
-- localGetCapacity() -- return the current capacity setting for this LDT
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
local function localGetCapacity( topRec, ldtBinName )
  local meth = "localGetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local ldtMap = ldtCtrl[2];
  local capacity = ldtMap[M_StoreLimit];
  if( capacity == nil ) then
    capacity = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function localGetCapacity()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- NOTE: Requirements/Restrictions (this version).
-- (1) One Set Per Record
-- ======================================================================
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) Object = get( topRec, ldtBinName, searchValue ) 
-- (*) Number  = exists( topRec, ldtBinName, searchValue ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================

-- ======================================================================
-- || create      || (deprecated)
-- || lset_create || (deprecated)
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
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) userModule: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- ======================================================================
function create( topRec, ldtBinName, userModule )
  GP=B and info("\n\n  >>>>>>>> API[ CREATE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetCreate( topRec, ldtBinName, userModule );
end

function lset_create( topRec, ldtBinName, userModule )
  return localLSetCreate( topRec, ldtBinName, userModule );
end

-- ======================================================================
-- add() -- Add an object to the LSET
-- lset_insert()  :: Deprecated
-- lset_create_and_insert()  :: Deprecated
-- ======================================================================
function add( topRec, ldtBinName, newValue, userModule )
  GP=B and info("\n\n  >>>>>>>> API[ ADD ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetInsert( topRec, ldtBinName, newValue, userModule )
end -- add()

function lset_insert( topRec, ldtBinName, newValue )
  return localLSetInsert( topRec, ldtBinName, newValue, nil )
end -- lset_insert()

function lset_create_and_insert( topRec, ldtBinName, newValue, userModule )
  return localLSetInsert( topRec, ldtBinName, newValue, userModule )
end -- lset_create_and_insert()

-- ======================================================================
-- add_all() -- Add a LIST of objects to the LSET.
-- lset_insert_all() :: Deprecated
-- ======================================================================
function add_all( topRec, ldtBinName, valueList )
  GP=B and info("\n\n  >>>>>>>> API[ ADD ALL ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetInsertAll( topRec, ldtBinName, valueList, nil );
end -- add_all()

function lset_insert_all( topRec, ldtBinName, valueList )
  return localLSetInsertAll( topRec, ldtBinName, valueList, nil );
end

function lset_create_and_insert_all( topRec, ldtBinName, valueList )
  return localLSetInsertAll( topRec, ldtBinName, valueList, userModule );
end

-- ======================================================================
-- get(): Return the object matching <searchValue>
-- get_with_filter() :: not currently exposed in the API
-- lset_search()
-- lset_search_then_filter()
-- ======================================================================
function get( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ GET ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetSearch( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- get()

function get_then_filter(topRec,ldtBinName,searchValue,userModule,filter,fargs)
  GP=B and info("\n\n  >>>>>>>> API[ GET THEN FILTER] <<<<<<<<<<<<<<<< \n");
  return localLSetSearch(topRec,ldtBinName,searchValue,userModule,filter,fargs);
end -- get_with_filter()

function lset_search( topRec, ldtBinName, searchValue )
  return localLSetSearch( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- lset_search()

function
lset_search_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetSearch(topRec, ldtBinName, searchValue, nil, filter, fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- exists() -- return 1 if item exists, otherwise return 0.
-- exists_with_filter() :: Not currently exposed in the API
-- lset_exists() -- with and without filter
-- ======================================================================
function exists( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ EXISTS ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetExists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function exists_with_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_with_filter()

function lset_exists( topRec, ldtBinName, searchValue )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function
lset_exists_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_then_filter()

-- ======================================================================
-- scan() -- Return a list containing ALL of LSET
-- lset_scan() :: Deprecated
-- ======================================================================
function scan( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ SCAN ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetScan(topRec,ldtBinName,nil, nil, nil);
end -- scan()

function lset_scan( topRec, ldtBinName )
  return localLSetScan(topRec,ldtBinName,nil, nil, nil);
end -- lset_search()

-- ======================================================================
-- filter() -- Return a list containing all of LSET that passed <filter>
-- lset_scan_then_filter() :: Deprecated
-- ======================================================================
function filter(topRec, ldtBinName, userModule, filter, fargs)
  GP=B and info("\n\n  >>>>>>>> API[ FILTER ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetScan(topRec, ldtBinName, userModule, filter, fargs);
end -- filter()

function lset_scan_then_filter(topRec, ldtBinName, filter, fargs)
  return localLSetScan(topRec, ldtBinName, nil, filter,fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- remove() -- remove <searchValue> from the LSET
-- take() -- remove and RETURN <searchValue> from the LSET
-- lset_delete() :: Deprecated
-- Return Status (OK or error)
-- ======================================================================
function remove( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- remove()

function take( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,true );
end -- remove()


function lset_delete( topRec, ldtBinName, searchValue )
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- lset_delete()

-- ======================================================================
-- remove_with_filter()
-- lset_delete_then_filter()
-- ======================================================================
function remove_with_filter( topRec, ldtBinName, searchValue, userModule,
  filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,userModule,
    filter,fargs,false);
end -- delete_then_filter()

function
lset_delete_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,filter,fargs,false);
end -- lset_delete_then_filter()

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lset_remove() :: Deprecated
-- ========================================================================
-- Completely remove this LDT: all data and the bin content.
-- If this is the LAST LDT in the record, then ALSO remove the
-- HIDDEN LDT CONTROL BIN.
-- ==>  Remove the ESR, Null out the topRec bin.  The rest will happen
-- during NSUP cleanup.
-- Parms:
-- (1) topRec: the user-level record holding the LSET Bin
-- (2) ldtBinName: The name of the LSET Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ DESTROY ] <<<<<<<<<<<<<<<<<< \n");
  return localLdtDestroy( topRec, ldtBinName );
end

function lset_remove( topRec, ldtBinName )
  return localLdtDestroy( topRec, ldtBinName );
end

-- ========================================================================
-- size() -- Return the number of objects in the LSET.
-- lset_size() :: Deprecated
-- ========================================================================
function size( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ SIZE ] <<<<<<<<<<<<<<<<<< \n");
  return localGetSize( topRec, ldtBinName );
end

function get_size( topRec, ldtBinName )
  return localGetSize( topRec, ldtBinName );
end

function lset_size( topRec, ldtBinName )
  return localGetSize( topRec, ldtBinName );
end

-- ========================================================================
-- get_config() -- return the config settings in the form of a map
-- lset_config() -- return the config settings in the form of a map
-- ========================================================================
function get_config( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ GET CONFIG ] <<<<<<<<<<<<<<<<<< \n");
  return localGetConfig( topRec, ldtBinName );
end

function lset_config( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ LSET CONFIG ] < (old api) <<<<<< \n");
  return localGetConfig( topRec, ldtBinName );
end

-- ========================================================================
-- get_capacity() -- return the current capacity setting for this LDT.
-- set_capacity() -- set the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function get_capacity( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ GET CAPACITY ] <<<<<<<<<<<<<<<<<< \n");
  return localGetCapacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  GP=B and info("\n\n  >>>>>>>> API[ SET CAPACITY ] <<<<<<<<<<<<<<<<<< \n");
  return localSetCapacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
-- ========================================================================
--
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- (*) debug()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function dump( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ DUMP ] <<<<<<<<<<<<<<<<<< \n");
  local meth = "dump()";
  GP=E and info("[ENTER]<%s:%s> LDT BIN(%s)", MOD, meth, tostring(ldtBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );
  warn("Function DUMP is CURRENTLY UNDER CONSTRUCTION");
  -- localDump(); -- Dump out our entire LDT structure.

  -- Another key difference between dump and scan : 
  -- dump prints things in the logs and returns a 0
  -- scan returns the list to the client/caller 

  local ret = " \n LDT bin contents dumped to server-logs \n"; 
  return ret; 
end -- dump();

-- ========================================================================
-- debug() -- turn on/off our debug settings
-- ========================================================================
function debug( topRec, setting )
  GP=B and info("\n\n  >>>>>>>> API[ DEBUG ] <<<<<<<<<<<<<<<<<< \n");
  return localDebug( topRec, setting );
end

-- ======================================================================
-- update_stats()
-- ======================================================================
-- This UDF manages a set of statistics for the user.  It tracks several
-- values (Min, Max, Ave, Sum, Count) for the user.
--
-- The statistics record has the following bins:
-- MaxValue: holds the largest number seen so far.
-- MinValue: holds the smallest number seen so far.
-- Sum:      holds the sum of what's been seen so far.
-- AveValue: holds the average value (computed)
-- Count:    holds the number of values that have been processed
--
-- Parms:
-- (*) rec: the main Aerospike Record
-- (*) newValue: the new value passed in by the user
-- Return:
-- (Success): 0
-- (Error): The error code returned by the "error()" call
-- ======================================================================
local function update_stats( rec, newValue )
  local meth = "update_stats()";
  trace("[ENTER]<%s> Value(%d)", meth, newValue);
  local rc = 0;

  -- Check for the record, create if not yet there.
  if( not aerospike_exists( rec ) ) then
    rc = aerospike:create( rec );
    if( rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    end
    rec["MaxValue"]   = newValue;
    rec["MinValue"]   = newValue;
    rec["Sum"]        = newValue;
    rec["AveValue"]   = newValue;
    rec["ValueCount"] = 1;
  else
    if( newValue > rec["MaxValue"] ) then
      rec["MaxValue"] = newValue;
    end
    if( newValue < rec["MinValue"] ) then
      rec["MinValue"] = newValue;
    end
    local count = rec["Count"];
    count = count + 1;
    rec["Count"] = count;
    local sum = rec["Sum"];
    sum = sum + newValue;
    rec["Sum"] = sum;
    rec["AveValue"] = sum / count;
  end -- else record already exists
  
  rc = aerospike:update( rec );
  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]:<%s> Problems updating the record", meth );
    error("ERROR updating the record");
  end
  rc = 0;
  trace("[EXIT]:<%s:%s> RC(0)", MOD, meth );
  return 0;
end -- localLSetInsert()

-- ======================================================================
-- localLSetInsertAll() -- with and without create
-- ======================================================================
-- ======================================================================
local function localLSetInsertAll( topRec, ldtBinName, valueList, userModule )
  local meth = "lset_insert_all()";
  local rc = 0;
  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      rc = localLSetInsert( topRec, ldtBinName, valueList[i], userModule );
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
end -- localLSetInsertAll()

-- ======================================================================
-- ======================================================================
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Large Set Exists
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return 1 item if the item exists in the set, otherwise return 0.
-- We don't want to return "true" and "false" because of Lua Weirdness.
-- Note that this looks a LOT like localLSetSearch(), except that we don't
-- return the object, nor do we apply a filter.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- ======================================================================
local function localLSetExists( topRec, ldtBinName, searchValue )

  local meth = "localLSetExists()";
  GP=E and trace("[ENTER]: <%s:%s> Search Value(%s)",
                 MOD, meth, tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject = 0;

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local key = getKeyValue( ldtMap, searchValue );

  -- Set up our global "UnTransform" and Filter Functions. This lets us
  -- process the function pointers once per call, and consistently for
  -- all LSET operations. (However, filter not used here.)
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, nil, nil, nil );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Search
    resultObject = subRecSearch( topRec, ldtCtrl, key );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, key );
  end

  local result = 1; -- be positive.
  if( resultObject == nil ) then
    result = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s>: Exists Result(%d)",MOD, meth, result ); 
  return result;
end -- function localLSetExist()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || localLSetSearch
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search), and optionally apply a filter.
-- Return the element if found, return an error (NOT FOUND) otherwise
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Optional Arguments to feed to the filter
-- Return the object, or Error (NOT FOUND)
-- ======================================================================
local function localLSetSearch( topRec, ldtBinName, searchValue,
        userModule, filter, fargs)

  local meth = "localLSetSearch()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) Search Value(%s)",
     MOD, meth, tostring( ldtBinName), tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject = 0;

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local key = getKeyValue( ldtMap, searchValue );

  -- Set up our global "UnTransform" and Filter Functions.  This lets us
  -- process the function pointers once per call, and consistently for
  -- all LSET operations.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Search
    resultObject = subRecSearch( topRec, ldtCtrl, key );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, key );
  end

  -- Report an error if we did not find the object.
  if( resultObject == nil ) then
    info("[NOT FOUND]<%s:%s> SearchValue(%s)",MOD,meth,tostring(searchValue));
    error(ldte.ERR_NOT_FOUND);
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)",MOD,meth,tostring(resultObject));
  return resultObject;
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
local function localLSetScan(topRec, ldtBinName, userModule, filter, fargs)
  local meth = "localLSetScan()";

  rc = 0; -- start out OK.
  GP=E and trace("[ENTER]<%s:%s> BinName(%s) Module(%s) Filter(%s) Fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(userModule), tostring(filter),
    tostring(fargs));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Find the appropriate bin for the Search value
  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultList = list();
  --
  -- Set up our global "UnTransform" and Filter Functions.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style scan
    subRecScan( topRec, ldtCtrl, resultList );
  else
    -- Use the TopRec style Scan (this is default Mode if the subrec style
    -- is not specifically requested).
    topRecScan( topRec, ldtCtrl, resultList );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s) Size : %d",
                 MOD, meth, tostring(resultList), list.size(resultList));

  return resultList; 
end -- function localLSetScan()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || topRecDelete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Top Record Mode
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtCtrl
-- (*) deleteValue:
-- (*) returnVal;  when true, return the deleted value.
-- ======================================================================
local function topRecDelete( topRec, ldtCtrl, deleteValue, returnVal)
  local meth = "topRecDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  -- Get the value we'll compare against
  local key = getKeyValue( ldtMap, deleteValue );

  -- Find the appropriate bin for the Search value
  local binNumber = computeSetBin( key, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local liveObject = nil;
  local resultFitlered = nil;
  local position = 0;

  -- We bother to search only if there's a real list.
  if binList ~= nil and list.size( binList ) > 0 then
    position = searchList( ldtCtrl, binList, key );
    if( position > 0 ) then
      -- Apply the filter to see if this item qualifies
      -- First -- we have to untransform it (sadly, again)
      local item = binList[position];
      if G_UnTransform ~= nil then
        liveObject = G_UnTransform( item );
      else
        liveObject = item;
      end

      -- APPLY FILTER HERE, if we have one.
      if filterFunction ~= nil then
        resultFiltered = filterFunction( liveObject, fargs );
      else
        resultFiltered = liveObject;
      end
    end -- if search found something (pos > 0)
  end -- if there's a list

  if( position == 0 or resultFiltered == nil ) then
    warn("[WARNING]<%s:%s> Value not found: Value(%s) SearchKey(%s)",
      MOD, meth, tostring(deleteValue), tostring(key));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok, we got the value.  Remove it and update the record.  Also,
  -- update the stats.
  -- OK -- we can't simply NULL out the entry -- because that breaks the bin
  -- when we try to store.  So -- we'll instead replace this current entry
  -- with the END entry -- and then we'll COPY (take) the list ... until
  -- we have the ability to truncate a list in place.
  local listSize = list.size( binList );
  if( position < listSize ) then
    binList[position] = binList[listSize];
  end
  local newBinList = list.take( binList, listSize - 1 );

  topRec[binName] = newBinList;
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

  -- The caller will update the stats and update the main ldt bin.

  GP=E and trace("[EXIT]<%s:%s>: Success: DeleteValue(%s) Res(%s) binList(%s)",
    MOD, meth, tostring( deleteValue ), tostring(resultFiltered),
    tostring(binList));
  if( returnVal == true ) then
    return resultFiltered;
  else
    return 0;
  end
end -- function topRecDelete()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) deleteValue:
-- (*) UserModule
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Arguments to feed to the filter
-- (*) returnVal: When true, return the deleted value.
-- ======================================================================
local function localLSetDelete( topRec, ldtBinName, deleteValue, userModule,
                                filter, fargs, returnVal)

  local meth = "localLSetDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[ldtBinName] == nil ) then
    GP=E and trace("[ERROR EXIT]: <%s:%s> LSetCtrlBin does not Exist",
                   MOD, meth );
    error( ldte.ERR_BIN_DOES_NOT_EXIST );
  end

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local resultObject;

  -- Get the value we'll compare against
  local key = getKeyValue( ldtMap, deleteValue );

  -- Set up our global "UnTransform" and Filter Functions.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style delete
    resultObject = subRecDelete( topRec, ldtCtrl, key, returnVal);
  else
    -- Use the TopRec style delete
    resultObject = topRecDelete( topRec, ldtCtrl, key, returnVal );
  end

  -- Update the Count, then update the Record.
  local itemCount = propMap[PM_ItemCount];
  propMap[PM_ItemCount] = itemCount - 1;
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  rc = aerospike:update( topRec );
  if( rc ~= nil and rc ~= 0 ) then
    warn("[WARNING]:<%s:%s> Bad Update Return(%s)", MOD, meth,tostring(rc));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]<%s:%s>: Success: DeleteValue(%s) Res(%s) binList(%s)",
    MOD, meth, tostring( deleteValue ), tostring(resultFiltered),
    tostring(binList));
  if( returnVal == true ) then
    return resultObject;
  else
    return 0;
  end
end -- function localLSetDelete()

-- ========================================================================
-- localGetSize() -- return the number of elements (item count) in the set.
-- ========================================================================
local function localGetSize( topRec, ldtBinName )
  local meth = "lset_size()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lset control structure
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  local itemCount = propMap[PM_ItemCount];

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function localGetSize()

-- ========================================================================
-- localGetConfig() -- return the config settings
-- ========================================================================
local function localGetConfig( topRec, ldtBinName )
  local meth = "localGetConfig()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
      MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local config = ldtSummary( topRec[ ldtBinName ] );

  GP=E and trace("[EXIT]:<%s:%s>:config(%s)", MOD, meth, tostring(config));

  return config;
end -- function localGetConfig()

-- ========================================================================
-- subRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function subRecDump( topRec, ldtCtrl )
  local meth = "subRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Loop through the Hash Directory
  warn("ERROR:: SUBREC DUMP NOT YET IMPLEMENTED!!!");

  GP=E and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

end -- subRecDump();


-- ========================================================================
-- topRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function topRecDump( topRec, ldtCtrl )
  local meth = "subRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Loop through all the modulo n lset-record bins 
  local distrib = ldtMap[M_Modulo];

  GP=F and trace(" Number of LSet bins to parse: %d ", distrib)

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
    trace("[DEBUG]<%s:%s> BIN(%s) TList(%s) B List(%s)", MOD, meth, binName,
      tostring(tempList), tostring(binList));
    list.append( resultList, binList );
  end -- end for distrib list for-loop 

  GP=E and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

end -- topRecDump();


-- ========================================================================
-- localDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function localDump( topRec, ldtBinName )
  local meth = "localDump()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)", MOD, meth,tostring(ldtBinName));

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  -- Check once for the untransform functions -- so we don't need
  -- to do it inside the loop.  No filters here, though.
  setKeyFunction( ldtMap, false )
  setReadFunctions( ldtMap, nil, nil, nil );

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy
    subRecDump( topRec, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    topRecDump( topRec, ldtCtrl );
  end

  local ret = " \n LSet bin contents dumped to server-logs \n"; 
  return ret; 
end -- localDump();

-- ========================================================================
-- topRecDestroy() -- Remove the LDT entirely from the TopRec LSET record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function topRecDestroy( topRec, ldtCtrl )
  local meth = "topRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));
  local rc = 0; -- start off optimistic

  -- The caller has already dealt with the Common/Hidden LDT Prop Bin.
  -- All we need to do here is deal with the Numbered bins.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  local ldtBinName = propMap[PM_BinName];

  -- Address the TopRecord version here.
  -- Loop through all the modulo n lset-record bins 
  -- Go thru and remove (mark nil) all of the LSET LIST bins.
  local distrib = ldtMap[M_Modulo];
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    -- Remove this bin -- assuming it is not already nil.  Setting a 
    -- non-existent bin to nil seems to piss off the lower layers. 
    if( topRec[binName] ~= nil ) then
        topRec[binName] = nil;
    end
  end -- end for distrib list for-loop 

  -- Mark the enitre control-info structure nil.
  topRec[ldtBinName] = nil;

end -- topRecDestroy()


-- ========================================================================
-- subRecDestroy() -- Remove the LDT (and subrecs) entirely from the record.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function subRecDestroy( topRec, ldtCtrl )
  local meth = "subRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));
  local rc = 0; -- start off optimistic

  -- Extract the property map and lso control map from the LDT Control
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), ldtSummaryString( ldtCtrl ));

  -- Get the ESR and delete it -- if it exists.  If we have ONLY an initial
  -- compact list, then the ESR will be ZERO.
  local esrDigest = propMap[PM_EsrDigest];
  if( esrDigest ~= nil and esrDigest ~= 0 ) then
    local esrDigestString = tostring(esrDigest);
    info("[SUBREC OPEN]<%s:%s> Digest(%s)", MOD, meth, esrDigestString );
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

  topRec[binName] = nil;

end -- subRecDestroy()


-- ========================================================================
-- localLdtDestroy() -- Remove the LDT entirely from the record.
-- NOTE: This could eventually be moved to COMMON, and be "localLdtDestroy()",
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
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function localLdtDestroy( topRec, ldtBinName )
  local meth = "localLdtDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and lso control map from the lso bin list.

  local ldtCtrl = topRec[ldtBinName]; -- The main lset map
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
      MOD, meth );
    error( ldte.ERR_INTERNAL );
  end
  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- This is the last LDT -- remove the LDT Control Property Bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  end

  if(ldtMap[M_SetTypeStore] ~= nil and ldtMap[M_SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy
    resultObject = subRecDestroy( topRec, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecDestroy( topRec, ldtCtrl );
  end

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
end -- localLdtDestroy()

-- ========================================================================
-- localSetCapacity() -- set the current capacity setting for this LDT
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
local function localSetCapacity( topRec, ldtBinName, capacity )
  local meth = "localSetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local ldtMap = ldtCtrl[2];
  if( capacity ~= nil and type(capacity) == "number" and capacity >= 0 ) then
    ldtMap[M_StoreLimit] = capacity;
  else
    warn("[ERROR]<%s:%s> Bad Capacity Value(%s)",MOD,meth,tostring(capacity));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]: <%s:%s> : new size(%d)", MOD, meth, capacity );

  return 0;
end -- function localSetCapacity()

-- ========================================================================
-- localGetCapacity() -- return the current capacity setting for this LDT
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
local function localGetCapacity( topRec, ldtBinName )
  local meth = "localGetCapacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and lso control map from the lso bin list.
  local ldtMap = ldtCtrl[2];
  local capacity = ldtMap[M_StoreLimit];
  if( capacity == nil ) then
    capacity = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function localGetCapacity()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- NOTE: Requirements/Restrictions (this version).
-- (1) One Set Per Record
-- ======================================================================
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) Object = get( topRec, ldtBinName, searchValue ) 
-- (*) Number  = exists( topRec, ldtBinName, searchValue ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================

-- ======================================================================
-- || create      || (deprecated)
-- || lset_create || (deprecated)
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
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) userModule: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- ======================================================================
function create( topRec, ldtBinName, userModule )
  GP=B and info("\n\n  >>>>>>>> API[ CREATE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetCreate( topRec, ldtBinName, userModule );
end

function lset_create( topRec, ldtBinName, userModule )
  return localLSetCreate( topRec, ldtBinName, userModule );
end

-- ======================================================================
-- add() -- Add an object to the LSET
-- lset_insert()  :: Deprecated
-- lset_create_and_insert()  :: Deprecated
-- ======================================================================
function add( topRec, ldtBinName, newValue, userModule )
  GP=B and info("\n\n  >>>>>>>> API[ ADD ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetInsert( topRec, ldtBinName, newValue, userModule )
end -- add()

function lset_insert( topRec, ldtBinName, newValue )
  return localLSetInsert( topRec, ldtBinName, newValue, nil )
end -- lset_insert()

function lset_create_and_insert( topRec, ldtBinName, newValue, userModule )
  return localLSetInsert( topRec, ldtBinName, newValue, userModule )
end -- lset_create_and_insert()

-- ======================================================================
-- add_all() -- Add a LIST of objects to the LSET.
-- lset_insert_all() :: Deprecated
-- ======================================================================
function add_all( topRec, ldtBinName, valueList )
  GP=B and info("\n\n  >>>>>>>> API[ ADD ALL ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetInsertAll( topRec, ldtBinName, valueList, nil );
end -- add_all()

function lset_insert_all( topRec, ldtBinName, valueList )
  return localLSetInsertAll( topRec, ldtBinName, valueList, nil );
end

function lset_create_and_insert_all( topRec, ldtBinName, valueList )
  return localLSetInsertAll( topRec, ldtBinName, valueList, userModule );
end

-- ======================================================================
-- get(): Return the object matching <searchValue>
-- get_with_filter() :: not currently exposed in the API
-- lset_search()
-- lset_search_then_filter()
-- ======================================================================
function get( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ GET ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetSearch( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- get()

function get_then_filter(topRec,ldtBinName,searchValue,userModule,filter,fargs)
  GP=B and info("\n\n  >>>>>>>> API[ GET THEN FILTER] <<<<<<<<<<<<<<<< \n");
  return localLSetSearch(topRec,ldtBinName,searchValue,userModule,filter,fargs);
end -- get_with_filter()

function lset_search( topRec, ldtBinName, searchValue )
  return localLSetSearch( topRec, ldtBinName, searchValue, nil, nil, nil);
end -- lset_search()

function
lset_search_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetSearch(topRec, ldtBinName, searchValue, nil, filter, fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- exists() -- return 1 if item exists, otherwise return 0.
-- exists_with_filter() :: Not currently exposed in the API
-- lset_exists() -- with and without filter
-- ======================================================================
function exists( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ EXISTS ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetExists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function exists_with_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_with_filter()

function lset_exists( topRec, ldtBinName, searchValue )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,nil, nil );
end -- lset_exists()

function
lset_exists_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetExists( topRec, ldtBinName, searchValue, nil,filter, fargs );
end -- lset_exists_then_filter()

-- ======================================================================
-- scan() -- Return a list containing ALL of LSET
-- lset_scan() :: Deprecated
-- ======================================================================
function scan( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ SCAN ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetScan(topRec,ldtBinName,nil, nil, nil);
end -- scan()

function lset_scan( topRec, ldtBinName )
  return localLSetScan(topRec,ldtBinName,nil, nil, nil);
end -- lset_search()

-- ======================================================================
-- filter() -- Return a list containing all of LSET that passed <filter>
-- lset_scan_then_filter() :: Deprecated
-- ======================================================================
function filter(topRec, ldtBinName, userModule, filter, fargs)
  GP=B and info("\n\n  >>>>>>>> API[ FILTER ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetScan(topRec, ldtBinName, userModule, filter, fargs);
end -- filter()

function lset_scan_then_filter(topRec, ldtBinName, filter, fargs)
  return localLSetScan(topRec, ldtBinName, nil, filter,fargs);
end -- lset_search_then_filter()

-- ======================================================================
-- remove() -- remove <searchValue> from the LSET
-- take() -- remove and RETURN <searchValue> from the LSET
-- lset_delete() :: Deprecated
-- Return Status (OK or error)
-- ======================================================================
function remove( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- remove()

function take( topRec, ldtBinName, searchValue )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,true );
end -- remove()


function lset_delete( topRec, ldtBinName, searchValue )
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,nil,nil,false);
end -- lset_delete()

-- ======================================================================
-- remove_with_filter()
-- lset_delete_then_filter()
-- ======================================================================
function remove_with_filter( topRec, ldtBinName, searchValue, userModule,
  filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,userModule,
    filter,fargs,false);
end -- delete_then_filter()

function
lset_delete_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return localLSetDelete(topRec,ldtBinName,searchValue,nil,filter,fargs,false);
end -- lset_delete_then_filter()

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lset_remove() :: Deprecated
-- ========================================================================
-- Completely remove this LDT: all data and the bin content.
-- If this is the LAST LDT in the record, then ALSO remove the
-- HIDDEN LDT CONTROL BIN.
-- ==>  Remove the ESR, Null out the topRec bin.  The rest will happen
-- during NSUP cleanup.
-- Parms:
-- (1) topRec: the user-level record holding the LSET Bin
-- (2) ldtBinName: The name of the LSET Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ DESTROY ] <<<<<<<<<<<<<<<<<< \n");
  return localLdtDestroy( topRec, ldtBinName );
end

function lset_remove( topRec, ldtBinName )
  return localLdtDestroy( topRec, ldtBinName );
end

-- ========================================================================
-- size() -- Return the number of objects in the LSET.
-- lset_size() :: Deprecated
-- ========================================================================
function size( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ SIZE ] <<<<<<<<<<<<<<<<<< \n");
  return localGetSize( topRec, ldtBinName );
end

function get_size( topRec, ldtBinName )
  return localGetSize( topRec, ldtBinName );
end

function lset_size( topRec, ldtBinName )
  return localGetSize( topRec, ldtBinName );
end

-- ========================================================================
-- get_config() -- return the config settings in the form of a map
-- lset_config() -- return the config settings in the form of a map
-- ========================================================================
function get_config( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ GET CONFIG ] <<<<<<<<<<<<<<<<<< \n");
  return localGetConfig( topRec, ldtBinName );
end

function lset_config( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ LSET CONFIG ] < (old api) <<<<<< \n");
  return localGetConfig( topRec, ldtBinName );
end

-- ========================================================================
-- get_capacity() -- return the current capacity setting for this LDT.
-- set_capacity() -- set the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function get_capacity( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ GET CAPACITY ] <<<<<<<<<<<<<<<<<< \n");
  return localGetCapacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  GP=B and info("\n\n  >>>>>>>> API[ SET CAPACITY ] <<<<<<<<<<<<<<<<<< \n");
  return localSetCapacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
-- ========================================================================
--
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- (*) debug()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function dump( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>> API[ DUMP ] <<<<<<<<<<<<<<<<<< \n");
  local meth = "dump()";
  GP=E and info("[ENTER]<%s:%s> LDT BIN(%s)", MOD, meth, tostring(ldtBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );
  warn("Function DUMP is CURRENTLY UNDER CONSTRUCTION");
  -- localDump(); -- Dump out our entire LDT structure.

  -- Another key difference between dump and scan : 
  -- dump prints things in the logs and returns a 0
  -- scan returns the list to the client/caller 

  local ret = " \n LDT bin contents dumped to server-logs \n"; 
  return ret; 
end -- dump();

-- ========================================================================
-- debug() -- turn on/off our debug settings
-- ========================================================================
function debug( topRec, setting )
  GP=B and info("\n\n  >>>>>>>> API[ DEBUG ] <<<<<<<<<<<<<<<<<< \n");
  return localDebug( topRec, setting );
end


-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
--   _      _____ _____ _____ 
--  | |    /  ___|  ___|_   _|
--  | |    \ `--.| |__   | |  
--  | |     `--. \  __|  | |  
--  | |____/\__/ / |___  | |  
--  \_____/\____/\____/  \_/  
--                            
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
--   _      _____ _____ _____ 
--  | |    /  ___|  ___|_   _|
--  | |    \ `--.| |__   | |  
--  | |     `--. \  __|  | |  
--  | |____/\__/ / |___  | |  
--  \_____/\____/\____/  \_/  
--                            
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
