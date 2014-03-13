-- Large Map (LMAP) Operations Library
-- Track the data and iteration of the last update.
local MOD="lib_lmap_2014_03_10.b";

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 2.1;

-- ======================================================================
-- || GLOBAL PRINT and GLOBAL DEBUG ||
-- ======================================================================
-- Use these flags to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- (*) "F" is used for general debug prints
-- (*) "E" is used for ENTER/EXIT prints
-- (*) "B" is used for BANNER prints
-- (*) DEBUG is used for larger structure content dumps.
-- ======================================================================
local GP;     -- Global Print Instrument
local F=true; -- Set F (flag) to true to turn ON global print
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=true; -- Set B (Banners) to true to turn ON Banner Print
local GD;     -- Global Debug instrument.
local DEBUG=true; -- turn on for more elaborate state dumps.

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LMAP Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LMAP library module:
--
-- (*) Status = lmap.create( topRec, ldtBinName, createSpec) 
-- (*) Status = lmap.put( topRec, ldtBinName, newName, newValue, userModule) 
-- (*) Status = lmap.put_all( topRec, ldtBinName, nameValueMap, userModule)
-- (*) Map    = lmap.get( topRec, ldtBinName, searchName )
-- (*) Map    = lmap.scan( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = lmap.remove( topRec, ldtBinName, searchName )
-- (*) Status = lmap.destroy( topRec, ldtBinName )
-- (*) Number = lmap.size( topRec, ldtBinName )
-- (*) Map    = lmap.config( topRec, ldtBinName )
-- (*) Status = lmap.set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Number = lmap.get_capacity( topRec, ldtBinName )
-- ======================================================================
--
-- Large Map Design/Architecture
--
-- The Large Map follows typical Map function, which is to say that it
-- contains a (potentially large) collection of name/value pairs.  These
-- name/value pairs are held in sub-record storage containers, which keeps
-- the amount of data stored in the main (top) record relatively small.
--
-- The Large Map design uses a single Bin (user-named LDT Bin) to hold
-- an LDT control structure that holds a Hash Directory.  The Hash directory
-- contains sub-record references (digests).  To locate a value, we hash
-- the name, follow the hash(name) modulo HashDirSize to a Hash Directory
-- Cell, and then search that sub-record for the name.
-- Each Subrecord contains two data lists, one for names and one for values.
-- ======================================================================
-- >> Please refer to ldt/doc_lmap.md for architecture and design notes.
-- ======================================================================

-- ======================================================================
-- -- Aerospike Database Server Functions:
-- -- ======================================================================
-- -- Aerospike Record Functions:
-- -- status = aerospike:create( topRec )
-- -- status = aerospike:update( topRec )
-- -- status = aerospike:remove( rec ) (not currently used)
-- --
-- -- Aerospike SubRecord Functions:
-- -- newRec = aerospike:create_subrec( topRec )
-- -- rec    = aerospike:open_subrec( topRec, digestString )
-- -- status = aerospike:update_subrec( childRec )
-- -- status = aerospike:close_subrec( childRec )
-- -- status = aerospike:remove_subrec( subRec ) 
-- --
-- -- Record Functions:
-- -- digest = record.digest( childRec )
-- -- status = record.set_type( topRec, recType )
-- -- status = record.set_flags( topRec, binName, binFlags )
-- -- ======================================================================
--
-- ++==================++
-- || External Modules ||
-- ++==================++
-- Set up our "outside" links.
-- Get addressability to the Function Table: Used for compress/transform,
-- keyExtract, Filters, etc. 
local functionTable = require('ldt/UdfFunctionTable');

-- When we're ready, we'll move all of our common routines into ldt_common,
-- which will help code maintenance and management.
-- local LDTC = require('ldt_common');

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt/ldt_errors');

-- We have a set of packaged settings for each LDT
local lmapPackage = require('ldt/settings_lmap');

-- Import our third party Hash Function:
local CRC32 = require('ldt/CRC32');

-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
-- This flavor of LDT
local LDT_TYPE_LMAP = "LMAP";

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE='T';
local AS_FALSE='F';

-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- The Hash Directory has a default starting size that can be overwritten.
local DEFAULT_HASH_MODULO = 32;

-- The Hash Directory has a "number of bits" (hash depth) that it uses to
-- to calculate calculate the current hash value.
local DEFAULT_HASH_DEPTH = 5; -- goes with 32, above.
--
-- Switch from a single list to distributed lists after this amount
local DEFAULT_THRESHOLD = 10;

local MAGIC="MAGIC";     -- the magic value for Testing LDT integrity

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY  ='B'; -- Using a Transform function to compact values
local SM_LIST    ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single cell list" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values -- this is more appropriate for LSET, but there is
-- still come code in here that references this.
-- TODO: Clean up code and stop referring to ATOMIC and COMPLEX.  All LMAP
-- name values are, by definition, simple and can't be complex.
local KT_ATOMIC  ='A'; -- the set value is just atomic (number or string)
local KT_COMPLEX ='C'; -- the set value is complex. Use Function to get key.

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

-- Enhancements for LMAP begin here 

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

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

-- We maintain a pool, or "context", of sub-records that are open.  That allows
-- us to look up sub-recs and get the open reference, rather than bothering
-- the lower level infrastructure.  There's also a limit to the number
-- of open sub-recs.
local G_OPEN_SR_LIMIT = 20;

---- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values. (There's no secret message hidden in these values).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- Trying to keep a consistent mapping across all LDT's : lstacks, lmap, lset 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common across all LDTs
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
local PM_SubRecCount           = 'S'; -- (Top): # of sub-recs in the LDT
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_CreateTime			   = 'C';
local PM_EsrDigest             = 'E'; -- (All): Digest of ESR
local PM_RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
local PM_LogInfo               = 'L'; -- (All): Log Info (currently unused)
local PM_ParentDigest          = 'P'; -- (Subrec): Digest of TopRec
local PM_SelfDigest            = 'D'; -- (Subrec): Digest of THIS Record

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LDT Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lmap 
local M_StoreMode              = 'M';-- List Mode or Binary Mode
local M_StoreLimit             = 'L'; -- Used for Eviction (eventually)
local M_UserModule             = 'P'; -- User's Lua file for overrides
local M_Transform              = 't'; -- Transform Lua to Byte format
local M_UnTransform            = 'u'; -- UnTransform from Byte to Lua format
local M_LdrEntryCountMax       = 'e'; -- Max # of items in an LDR
local M_LdrByteEntrySize       = 's';
local M_LdrByteCountMax        = 'b';
local M_StoreState             = 'S';-- "Compact List" or "Regular Hash"
local M_BinaryStoreSize        = 'B'; 
local M_KeyType                = 'K'; 
local M_TotalCount             = 'N';-- Total insert count (not counting dels)
local M_HashDirSize            = 'O';-- Show current Hash Dir Size
local M_HashDirMark            = 'm';-- Show where we are in the linear hash
local M_Threshold              = 'H';
local M_KeyFunction            = 'K'; -- User Supplied Key Extract Function
local M_CompactNameList        = 'n';--Simple Compact List -- before "dir mode"
local M_CompactValueList       = 'v';--Simple Compact List -- before "dir mode"

local M_OverWrite              = 'o';-- Allow Overwrite of a Value for a given
                                     -- name.  If false (AS_FALSE), then we
                                     -- throw a UNIQUE error.

-- Fields specific to lmap in the standard mode only. In standard mode lmap 
-- does not resemble lset, it looks like a fixed-size warm-list from lstack
-- with a digest list pointing to LDR's. 

local M_HashDirectory          = 'W';-- The Directory of Hash Entries
local M_HashCellMaxList        = 'X';-- Max List size in a Cell anchor
local M_TopFull                = 'F';
local M_ListDigestCount        = 'l';
local M_ListMax                = 'w';
-- lmap in standard mode is a fixed-size warm-list, so there is no need for
-- transfer-counters and the other associated stuff.  
-- local M_ListTransfer        = 'x'; 
-- 
-- count of the number of LDR's pointed to by a single digest entry in lmap
-- Is this a fixed-size ? (applicable only in standard mode) 
local M_TopChunkByteCount      = 'a'; 
--
-- count of the number of bytes present in top-most LDR from above. 
-- Is this a fixed-size ? (applicable only in standard mode) 
local M_TopChunkEntryCount = 'A';

-- ------------------------------------------------------------------------
-- Maintain the LDT letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
--
-- A:                         a:                        0:
-- B:M_BinaryStoreSize        b:M_LdrByteCountMax       1:
-- C:                         c:                        2:
-- D:                         d:                        3:
-- E:                         e:M_LdrEntryCountMax      4:
-- F:M_TopFull                f:                        5:
-- G:                         g:                        6:
-- H:M_Thresold               h:                        7:
-- I:                         i:                        8:
-- J:                         j:                        9:
-- K:M_KeyFunction            k:                  
-- L:M_StoreLimit             l:M_ListDigestCount
-- M:M_StoreMode              m:M_HashDirMark
-- N:M_TotalCount             n:M_CompactNameList
-- O:                         o:
-- P:M_UserModule             p:
-- Q:                         q:
-- R:M_ColdDataRecCount       r:
-- S:M_StoreLimit             s:M_LdrByteEntrySize
-- T:                         t:M_Transform
-- U:                         u:M_UnTransform
-- V:                         v:M_CompactValueList
-- W:M_HashDirectory          w:                     
-- X:                         x:                    
-- Y:                         y:
-- Z:                         z:
-- -----------------------------------------------------------------------
-- Cell Anchor Fields:  A cell anchor is a map object that sits in each
-- cell of the hash directory.   Since we don't have the freedom of keeping
-- NULL array entries (as one might in C), we have to keep an active object
-- in the Hash Directory list, otherwise, a NULL (nil) entry would actually
-- crash in message pack (or, somewhere).
--
-- A Hash Cell can be in one of FOUR states:
-- (1) C_EMPTY: just the CellState has a value.
-- (2) C_LIST: a small list of objects is anchored to this cell.
-- (3) C_DIGEST: A SINGLE DIGEST value points to a single sub-record.
-- (4) C_TREE: A Tree Root points to a set of Sub-Records
-- -----------------------------------------------------------------------
-- Here are the fields used in a Hash Cell Anchor
local C_CellState      = 'S'; -- Hold the Cell State
local C_CellNameList   = 'N'; -- Pt to a LIST of objects
local C_CellValueList  = 'V'; -- Pt to a LIST of objects
local C_CellDigest     = 'D'; -- Pt to a single digest value
local C_CellTree       = 'T'; -- Pt to a LIST of digests

-- Here are the various constants used with Hash Cells
local C_STATE_EMPTY   = 'E'; -- 
local C_STATE_LIST    = 'L'; 
local C_STATE_DIGEST  = 'D';
local C_STATE_TREE    = 'T';
-- -----------------------------------------------------------------------
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
-- (1) LDR (LDT Data Record) -- used in both the Warm and Cold Lists
-- (2) ColdDir Record -- used to hold lists of LDRs (the Cold List Dirs)
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- Note the 14 character limit on Aerospike Bin Names.
--                         123456789ABCDE
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_NLIST_BIN     = "LdrNListBin";  
local LDR_VLIST_BIN     = "LdrVListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin.
local SUBREC_PROP_BIN="SR_PROP_BIN";
--
-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)


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
    warn("[ERROR]<%s:%s> Key Function is Required for LLIST Complex Objects",
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
  GP=E and trace("[DEBUG]<%s:%s> UserMod(%s) Fargs(%s)",
    MOD, meth, tostring(userModule), tostring(filterArgs));

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
      -- If we didn't find a good filter then keep looking. 
      -- Try the createModule.
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

-- =========================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- =========================================================================

-- -----------------------------------------------------------------------
-- ------------------------------------------------------------------------
-- =============================
-- Begin SubRecord Function Area (MOVE THIS TO LDT_COMMON)
-- =============================
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, will employ the "subrecContext" object, which
-- will hold all of the sub-records that were opened during processing. 
-- Note that some operations can potentially involve many sub-rec
-- operations -- and can also potentially revisit pages.
--
-- SubRecContext Design:
-- The key will be the DigestString, and the value will be the subRec
-- pointer.  At the end of an outer call, we will iterate thru the sub-rec
-- context and close all open sub-records.  Note that we may also need
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
-- Given an already opened sub-rec (probably one that was recently created),
-- add it to the sub-rec context.
-- ======================================================================
local function addSubrecToContext( srcList, subRec )
  local meth = "addSubrecContext()";
  GP=E and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  if( srcList == nil ) then
    warn("[ERROR]<%s:%s> Bad Subrec Context: SRC is NIL", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  local digest = record.digest( subRec );
  local digestString = tostring( digest );
  recMap[digestString] = subRec;

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

  -- We have a global limit on the number of sub-recs that we can have
  -- open at a time.  If we're at (or above) the limit, then we must
  -- exit with an error (better here than in the sub-rec code).
  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local itemCount = recMap.ItemCount;

  local subRec = recMap[digestString];
  if( subRec == nil ) then
    if( itemCount >= G_OPEN_SR_LIMIT ) then
      warn("[ERROR]<%s:%s> SRC Count(%d) Exceeded Limit(%d)", MOD, meth,
        itemCount, G_OPEN_SR_LIMIT );
      error( ldte.ERR_TOO_MANY_OPEN_SUBRECS );
    end

    recMap.ItemCount = itemCount + 1;
    GP=F and trace("[OPEN SUBREC]<%s:%s>SRC.ItemCount(%d) TR(%s) DigStr(%s)",
      MOD, meth, recMap.ItemCount, tostring(topRec), digestString);
    subRec = aerospike:open_subrec( topRec, digestString );
    GP=F and trace("[OPEN SUBREC RESULTS]<%s:%s>(%s)", 
      MOD,meth,tostring(subRec));
    if( subRec == nil ) then
      warn("[ERROR]<%s:%s> Subrec Open Failure: Digest(%s)", MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
  else
    GP=F and trace("[FOUND REC]<%s:%s>Rec(%s)", MOD, meth, tostring(subRec));
  end

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
    MOD, meth, tostring(subRec), digestString );
  return subRec;
end -- openSubrec()


-- ======================================================================
-- closeSubrec()
-- ======================================================================
-- Close the sub-record -- providing it is NOT dirty.  For all dirty
-- sub-records, we have to wait until the end of the UDF call, as THAT is
-- when all dirty sub-records get written out and closed.
-- ======================================================================
local function closeSubrec( srcList, digestString )
  local meth = "closeSubrec()";
  GP=E and trace("[ENTER]<%s:%s> DigestStr(%s) SRC(%s)",
    MOD, meth, digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local itemCount = recMap.ItemCount;
  local rc = 0;

  local subRec = recMap[digestString];
  local dirtyStatus = dirtyMap[digestString];
  if( subRec == nil ) then
    warn("[INTERNAL ERROR]<%s:%s> Rec not found for Digest(%s)", MOD, meth,
      digestString );
    return rc;
    -- error( ldte.ERR_INTERNAL );
  end

  GP=F trace("[STATUS]<%s:%s> Closing Rec: Digest(%s)", MOD, meth, digestString);

  if( dirtyStatus == true ) then
    warn("[WARNING]<%s:%s> Can't close Dirty Record: Digest(%s)",
      MOD, meth, digestString);
  else
    rc = aerospike:close_subrec( subRec );
    GP=F and trace("[STATUS]<%s:%s>Closed Rec: Digest(%s) rc(%s)", MOD, meth,
      digestString, tostring( rc ));
  end

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subRec), digestString, tostring(rc));
  return rc;
end -- closeSubrec()


-- ======================================================================
-- updateSubrec()
-- ======================================================================
-- Update the sub-record -- and then mark it dirty.
-- ======================================================================
local function updateSubrec( srcList, subRec, digest )
  local meth = "updateSubrec()";
  --GP=E and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
 --   MOD, meth, tostring(topRec), digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local rc = 0;

  if( digest == nil or digest == 0 ) then
    digest = record.digest( subRec );
  end
  local digestString = tostring( digest );

  rc = aerospike:update_subrec( subRec );
  dirtyMap[digestString] = true;

  GP=E and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subRec), digestString, tostring(rc));
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

  -- Iterate thru the SubRecContext and close all sub-records.
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
      -- GP=F and info("[DEBUG]<%s:%s>: Closing SubRec: Digest(%s) Rec(%s)",
      --   MOD, meth, digestString, tostring(rec) );
      -- rc = aerospike:close_subrec( rec );
      -- GP=F and info("[DEBUG]<%s:%s>: Closing Results(%d)", MOD, meth, rc );
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
-- getKeyValue()
-- ======================================================================
-- This is left-over from the LSET code, where we might actually use a
-- function to extract an atomic value from a complex object.  That does
-- not apply to LMAP, where the Name field is always an atomic value, either
-- STRING or NUMBER.  If we get a COMPLEX value, then that's just an error.
-- ======================================================================
local function getKeyValue( ldtMap, value )
  local meth = "getKeyValue()";
  GP=E and trace("[ENTER]<%s:%s> value(%s)",
       MOD, meth, tostring(value) );

  GP=F and trace(" Ctrl-Map : %s", tostring(ldtMap));

  local keyValue;
  if ldtMap[M_KeyType] == KT_ATOMIC then
    keyValue = value;
  else
    warn("[ERROR]<%s:%s> LMAP can only have Number or String values for Name",
      MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue));
  return keyValue;
end -- getKeyValue();

-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the lsetMap, if present, and apply
-- it to the value, returning the transformed value.  If no transform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyTransform( transformFunc, newValue )
  local meth = "applyTransform()";
  GP=E and trace("[ENTER]<%s:%s> transform(%s) type(%s) Value(%s)", MOD,
    meth, tostring(transformFunc), type(transformFunc), tostring(newValue));

  local storeValue = newValue;
  if transformFunc ~= nil then 
    storeValue = transformFunc( newValue );
  end
  return storeValue;
end -- applyTransform()

-- =======================================================================
-- Apply UnTransform Function
-- Take the UnTransform defined in the lsetMap, if present, and apply
-- it to the dbValue, returning the unTransformed value.  If no unTransform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyUnTransform( ldtMap, storeValue )
  local returnValue = storeValue;
  if ldtMap[M_UnTransform] ~= nil and
    functionTable[ldtMap[M_UnTransform]] ~= nil then
    returnValue = functionTable[ldtMap[M_UnTransform]]( storeValue );
  end
  return returnValue;
end -- applyUnTransform( value )

-- =======================================================================
-- unTransformSimpleCompare()
-- Apply the unTransform function to the DB value and compare the transformed
-- value with the searchKey.
-- Return the unTransformed DB value if the values match.
-- =======================================================================
local function unTransformSimpleCompare(unTransform, dbValue, searchKey)
  local modValue = dbValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modValue = unTransform( dbValue );
  end

  if modValue == searchKey then
    resultValue = modValue;
  end

  return resultValue;
end -- unTransformSimpleCompare()

-- =======================================================================
-- unTransformComplexCompare()
-- Apply the unTransform function to the DB value, extract the key,
-- then compare the values, using simple equals compare.
-- Return the unTransformed DB value if the values match.
-- parms:
-- (*) lsetMap
-- (*) trans: The transformation function: Perform if not null
-- (*) dbValue: The value pulled from the DB
-- (*) searchValue: The value we're looking for.
-- =======================================================================
local function
unTransformComplexCompare(ldtMap, unTransform, dbValue, searchKey)
  local meth = "unTransformComplexCompare()";

  GP=E and trace("[ENTER]<%s:%s> unTransform(%s) dbVal(%s) key(%s)",
     MOD, meth, tostring(unTransform), tostring(dbValue), tostring(searchKey));

  local modValue = dbValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modValue = unTransform( dbValue );
  end
  
  if modValue == searchKey then
    resultValue = modValue;
  end

  return resultValue;
end -- unTransformComplexCompare()

-- ======================================================================
-- propMapSummary( resultMap, propMap )
-- ======================================================================
-- Add the propMap properties to the supplied resultMap.
-- ======================================================================
local function propMapSummary( resultMap, propMap )

  -- Fields common for all LDT's
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropSubRecCount      = propMap[PM_SubRecCount];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.PropMagic            = propMap[PM_Magic];
  resultMap.PropCreateTime       = propMap[PM_CreateTime];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  resultMap.RecType              = propMap[PM_RecType];
  resultMap.ParentDigest         = propMap[PM_ParentDigest];
  resultMap.SelfDigest           = propMap[PM_SelfDigest];
end -- function propMapSummary()
  
-- ======================================================================
-- ldtMapSummary( resultMap, ldtMap )
-- ======================================================================
-- Add the LDT Map properties to the supplied resultMap.
-- ======================================================================
local function ldtMapSummary( resultMap, ldtMap )

  -- General LMAP Parms:
  resultMap.StoreMode            = ldtMap[M_StoreMode];
  resultMap.Transform            = ldtMap[M_Transform];
  resultMap.UnTransform          = ldtMap[M_UnTransform];
  resultMap.UserModule           = ldtMap[M_UserModule];
  resultMap.BinaryStoreSize      = ldtMap[M_BinaryStoreSize];
  resultMap.KeyType              = ldtMap[M_KeyType];
  resultMap.TotalCount	         = ldtMap[M_TotalCount];		
  resultMap.HashDirSize          = ldtMap[M_HashDirSize];
  resultMap.ThreshHold		     = ldtMap[M_Threshold];
  
  -- LDT Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = ldtMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[M_LdrByteCountMax];

  -- Digest List Settings: List of Digests of LMAP Data Records
  -- specific to LMAP in STANDARD_MODE ONLY 
  
  resultMap.TopFull 	      = ldtMap[M_TopFull];
  resultMap.ListDigestCount   = ldtMap[M_ListDigestCount];
  resultMap.ListMax           = ldtMap[M_ListMax];
  resultMap.TopChunkByteCount = ldtMap[M_TopChunkByteCount];
  resultMap.TopChunkEntryCount= ldtMap[M_TopChunkEntryCount];
end -- function ldtMapSummary


-- ======================================================================
-- ldtDebugDump()
-- ======================================================================
-- To aid in debugging, dump the entire contents of the ldtCtrl object
-- for LMAP.  Note that this must be done in several prints, as the
-- information is too big for a single print (it gets truncated).
-- ======================================================================
local function ldtDebugDump( ldtCtrl )

  -- Print MOST of the "TopRecord" contents of this LMAP object.
  local resultMap                = map();
  resultMap.SUMMARY              = "LMAP Summary";

  info("\n\n <><><><><><><><><> [ LDT LMAP SUMMARY ] <><><><><><><><><> \n");

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  
  if( propMap[PM_Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN MAP--No Magic";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );
  info("\n<<<%s>>>\n", tostring(resultMap));
  resultMap = nil;

  -- Reset for each section, otherwise the result would be too much for
  -- the info call to process, and the information would be truncated.
  resultMap2 = map();
  resultMap2.SUMMARY              = "LMAP-SPECIFIC Values";

  -- Load the LMAP-specific properties
  ldtMapSummary( resultMap2, ldtMap );
  info("\n<<<%s>>>\n", tostring(resultMap2));
  resultMap2 = nil;

  -- Print the Hash Directory
  resultMap3 = map();
  resultMap3.SUMMARY              = "LMAP Hash Directory";
  resultMap3.HashDirectory        = ldtMap[M_HashDirectory];
  info("\n<<<%s>>>\n", tostring(resultMap3));

end -- function ldtDebugDump()

-- ======================================================================
-- local function ldtSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtCtrl 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function ldtSummary( ldtCtrl )
  local meth = "ldtSummary()";

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();
  resultMap.SUMMARY              = "LMAP Summary";

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    return resultMap;
  end

  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  
  if( propMap[PM_Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN MAP--No Magic";
    return resultMap;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );

  -- Load the LMAP-specific properties
  ldtMapSummary( resultMap, ldtMap );

  return resultMap;
end -- ldtSummary()

-- ======================================================================
-- Make it easier to use ldtSummary(): Have a String version.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
    return tostring( ldtSummary( ldtCtrl ) );
end

-- ======================================================================
-- local function ldtMapSummary( ldtMap )
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function ldtMapDump( ldtMap )
  local meth = "ldtMapDump()";

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();
  resultMap.SUMMARY              = "LMAP Summary";

  if ( ldtMap == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT MAP!!", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT MAP.";
    return resultMap;
  end

  ldtMapSummary( resultMap, ldtMap );

  return resultMap;
end -- ldtMapDump()

-- ======================================================================
-- Make it easier to use ldtMapSummary(): Have a String version.
-- ======================================================================
local function ldtMapSummaryString( ldtMap )
    return tostring( ldtMapDump( ldtMap ) );
end

-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- ======================================================================
local function setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth);

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if( topRec[REC_LDT_CTRL_BIN] == nil ) then
    GP=F and trace("[DEBUG]<%s:%s>Creating Record LDT Map", MOD, meth);

    -- If this record doesn't even exist yet -- then create it now.
    -- Otherwise, things break.
    if( not aerospike:exists( topRec ) ) then
      GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth);
      rc = aerospike:create( topRec );
    end

    record.set_type( topRec, RT_LDT );
    recPropMap = map();
    -- vinfo will be a 5 byte value, but it will be easier for us to store
    -- 6 bytes -- and just leave the high order one at zero.
    -- Initialize the VINFO value to all zeros.
    --local vinfo = bytes(6);
    --bytes.put_int16(vinfo, 1, 0 );
    --bytes.put_int16(vinfo, 3, 0 );
    --bytes.put_int16(vinfo, 5, 0 );
    local vinfo = 0; 
    recPropMap[RPM_VInfo] = vinfo; 
    recPropMap[RPM_LdtCount] = 1; -- this is the first one.
    recPropMap[RPM_Magic] = MAGIC;
  --  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_CONTROL );
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM_LdtCount];
    recPropMap[RPM_LdtCount] = ldtCount + 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    GP=F and trace("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1);
  end

  topRec[REC_LDT_CTRL_BIN] = recPropMap;    
  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );
    if( rc == nil or rc == 0 ) then
      GP=E and trace("[EXIT]: <%s:%s>", MOD, meth);      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 

  GP=E and trace("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc);
  return rc;
end -- setLdtRecordType()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two main Record Types used in the LDT Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LDT bin
-- (*) ldtBinName: the LDT Data Record that holds user Data
-- (*) compact_mode_flag : decides LMAP storage mode : SS_COMPACT or SS_REGULAR
--
-- <+> Naming Conventions:
--   + All Field names (e.g. M_StoreMode) begin with Upper Case
--   + All variable names (e.g. ldtMap) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[ldtBinName] or ldrRec[LDR_CTRL_BIN]);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ======================================================================
-- initializeLdtCtrl: (LMAP)
-- ======================================================================
-- Set up the LMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LDT BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LMAP
-- behavior.  Thus this function represents the "type" LMAP -- all
-- LMAP control fields are defined here.
-- The LMap is obtained using the user's LMap Bin Name:
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) namespace: The Namespace of the record (topRec)
-- (*) set: The Set of the record (topRec)
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized ldtCtrl structure.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLdtCtrl( topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  
  -- Create 2 maps : The generic property map 
  -- and lmap specific property map. Create one
  -- list : the actual LDR list for lmap. 
  -- Note: All Field Names start with UPPER CASE.
  local ldtMap = map();
  local propMap = map(); 
  local ldtCtrl = list(); 
  
  GP=E and trace("[ENTER]<%s:%s>::ldtBinName(%s)",
      MOD, meth, tostring(ldtBinName));

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM_SubRecCount] = 0;
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LMAP; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]  = nil; -- not set yet.
  propMap[PM_SelfDigest] = nil; 
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_CreateTime] = aerospike:get_current_time();
  -- warn("WARNING:: Please Fix GET CURRENT TIME");
  -- propMap[PM_CreateTime] = 0;
  
-- Specific LMAP Parms: Held in LMap
  ldtMap[M_StoreMode]  = SM_LIST; -- SM_LIST or SM_BINARY:
  ldtMap[M_StoreLimit]  = nil; -- No storage Limit

  -- LMAP Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[M_LdrByteEntrySize] =  0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  =   0; -- Max # of Data Chunk Bytes (binary mode)

  ldtMap[M_Transform]        = nil; -- applies only to complex lmap
  ldtMap[M_UnTransform]      = nil; -- applies only to complex lmap
  ldtMap[M_StoreState]       = SS_COMPACT; -- Start out in "single list" mode
  ldtMap[M_HashType]         = HT_STATIC; -- Static or Dynamic
  ldtMap[M_BinaryStoreSize]  = nil; 
  ldtMap[M_KeyType]          = KT_ATOMIC; -- assume "atomic" values for now.
  ldtMap[M_TotalCount]       = 0; -- Count of both valid and deleted elements
  ldtMap[M_HashDirSize]      = DEFAULT_HASH_MODULO; -- Hash Dir Size
  ldtMap[M_HashDepth]        = DEFAULT_HASH_DEPTH; -- # of hash bits to use
  -- Rehash after this many have been inserted
  ldtMap[M_Threshold]       = DEFAULT_THRESHOLD;
  -- name-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[M_CompactNameList]  = list();
  -- value-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[M_CompactValueList] = list();

  -- We allow or do NOT allow overwrites of values for a given name.
  ldtMap[M_OverWrite] = AS_TRUE; -- Start out flexible.


  -- Just like we have a COMPACT LIST for the entire Hash Table, we also
  -- have small lists that we'll keep in each Hash Cell -- to keep us from
  -- allocating a subrecord for just one or two items.  As soon as we pass
  -- the threshold (e.g. 4), then we'll convert to a sub-record.
  ldtMap[M_HashCellMaxList]   = 4; --Keep lists small in the cells.
	  
  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  setLdtRecordType( topRec );

  -- Put our new maps in a list, in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  -- Once this list of 2 maps is created, we need to assign it to topRec
  topRec[ldtBinName]            = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]<%s:%s> : LMAP Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth);
  return ldtCtrl;
  
end -- initializeLdtCtrl()

-- ======================================================================
-- initializeLMapRegular()
-- ======================================================================
-- Set up the ldtCtrl map for REGULAR use (sub-records).
-- ======================================================================
local function initializeLMapRegular( topRec, ldtCtrl )
  local meth = "initializeLMapRegular()";
  
  GP=E and info("[ENTER]: <%s:%s>:: Regular Mode", MOD, meth );
  
  -- Extract the property map and LDT control map from the LDT bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Reset the Prop and LDT Maps to settings appropriate for the REGULAR
  -- storage mode (i.e. using sub-records).
  -- All the other params must already be set by default. 
  local ldtBinName = propMap[PM_BinName];
 
  GP=F and info("[DEBUG]<%s:%s> Regular-Mode ldtBinName(%s) Key-type: %s",
      MOD, meth, tostring(ldtBinName), tostring(ldtMap[M_KeyType]));

  ldtMap[M_StoreState]  = SS_REGULAR; -- SM_LIST or SM_BINARY:
  	  
  -- Setup and Allocate everything for the Hash Directory.
  local newDirList = list(); -- Our new Hash Directory
  local hashDirSize = ldtMap[M_HashDirSize];
  local cellAnchor;
  for i = 1, (hashDirSize + 1), 1 do
    cellAnchor = map();
    cellAnchor[C_CellState] = C_STATE_EMPTY;
    list.append( newDirList, cellAnchor );
  end

  ldtMap[M_HashDirectory]        = newDirList;
  
  -- true when the list of entries pointed to by a digest is full
  -- (for next write).
  -- When this flag is set, we'll do a new chunk-create + new digest entry in 
  -- digest-list vs simply an entry-add to the list
  ldtMap[M_TopFull] = false; 
  
  -- How many LDR chunks (entry lists) exist in this lmap bin 
  ldtMap[M_ListDigestCount]   = 0; -- Number of Warm Data Record Chunks
      
  -- This field is technically used to determine if warm-list has any more room 
  -- of if we want to age and transfer some items to cold-list to make room. 
  -- Since there is no overflow, this might not be needed really ? or we can 
  -- reuse it to determine something else -- Check with Toby
      
  ldtMap[M_ListMax]           = 100; -- Max Number of Data Record Chunks
  ldtMap[M_TopChunkEntryCount]= 0; -- Count of entries in top chunks
  ldtMap[M_TopChunkByteCount] = 0; -- Count of bytes used in top Chunk

  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  GP=F and info("[DEBUG]<%s:%s> LMAP Summary after Init(%s)",
       MOD, meth, ldtMapSummaryString(ldtMap));

  GP=E and info("[EXIT]:<%s:%s>:", MOD, meth );
  
end -- function initializeLMapRegular

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( ldtBinName )
  local meth = "validateBinName()";
  GP=E and info("[ENTER]: <%s:%s> validate Bin Name(%s)",
      MOD, meth, tostring(ldtBinName));

  if ldtBinName == nil  then
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( ldtBinName ) ~= "string"  then
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( ldtBinName ) > 14 then
    error( ldte.ERR_BIN_NAME_TOO_LONG );
  end
end -- validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- ======================================================================
-- Check that the topRec, the ldtBinName and ldtMap are valid, otherwise
-- jump out with an error() call.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName: User's Name for the LDT Bin
-- (*) mustExist: When true, ldtCtrl must exist, otherwise error
-- Return:
--   ldtCtrl -- if "mustExist" is true, otherwise unknown.
-- ======================================================================
local function validateRecBinAndMap( topRec, ldtBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=E and info("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( ldtBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  validateBinName( ldtBinName );

  local ldtCtrl;
  local propMap;
  local ldtMap;

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid Bin
  -- (*) Must have a valid Map in the bin.
  --
  -- Otherwise, If "mustExist" is false, then basically we're just going
  -- to check that our bin includes MAGIC, if it is non-nil.
  -- TODO : Flag is true for peek, trim, config, size, delete etc 
  -- Those functions must be added b4 we validate this if section 

  if mustExist == true then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
     
    -- Control Bin Must Exist, in this case, ldtCtrl is what we check.
    if( topRec[ldtBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LMAP BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    ldtCtrl = topRec[ldtBinName] ; -- The main LdtMap structure

    -- Extract the property map and Ldt control map from the Ldt bin list.
    propMap = ldtCtrl[1];
    ldtMap  = ldtCtrl[2];

    if propMap[PM_Magic] ~= MAGIC then
      GP=E and warn("[ERROR EXIT]:<%s:%s>LMAP BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( ldtBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    
    if topRec ~= nil and topRec[ldtBinName] ~= nil then
      ldtCtrl = topRec[ldtBinName]; -- The main LdtMap structure
      -- Extract the property map and Ldt control map from the Ldt bin list.
      propMap = ldtCtrl[1];
      ldtMap  = ldtCtrl[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=E and warn("[ERROR EXIT]:<%s:%s> LMAP BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( ldtBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
    end -- if worth checking
  end -- else for must exist

  -- Finally -- let's check the version of our code against the version
  -- in the data.  If there's a mismatch, then kick out with an error.
  --  Can't do version control this way becase we don't FUCKING store
  --  real numbers.  We'll have to store MAJOR and MINOR separately.
  --  MAJOR will be a data format change -- and so we may not support
  --  data changes across MAJOR versions.
--  if( G_LDT_VERSION > propMap[PM_Version] ) then
--    GP=E and warn("[ERROR EXIT]:<%s:%s> Code Version (%f) <> Data Version(%f)",
--    MOD, meth, G_LDT_VERSION, propMap[PM_Version]);
--    error( ldte.ERR_VERSION_MISMATCH );
--  end
 
  return ldtCtrl; -- Save the caller the effort of extracting the map.

end -- validateRecBinAndMap()

-- ======================================================================
-- adjustLdtMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LdtMap:
-- Parms:
-- (*) ldtCtrl: the main Ldt Bin value (propMap, ldtMap)
-- (*) argListMap: Map of Ldt Settings 
-- Return: The updated LdtList
-- ======================================================================
local function adjustLdtMap( ldtCtrl, argListMap )
  local meth = "adjustLdtMap()";
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  GP=E and trace("[ENTER]: <%s:%s>:: LdtCtrl(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtCtrl), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the stackCreate() call.
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
      local ldtPackage = lmapPackage[value];
      if( ldtPackage ~= nil ) then
        ldtPackage( ldtMap );
      end
    end
  end -- for each argument

  GP=E and trace("[EXIT]:<%s:%s>:LdtCtrl after Init(%s)",
    MOD,meth,tostring(ldtCtrl));
  return ldtCtrl;
end -- adjustLdtMap

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

-- =======================================================================
-- searchList()
-- =======================================================================
-- Search a list for an item.  Given that this is LMAP, we're searching
-- for a "Name", which is a SIMPLE type and is our searchKey.
--
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) binList: the list of values from the record
-- (*) searchKey: the "value"  we're searching for
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
    if item ~= nil and item == searchKey then
      position = i;
      break;
    end -- end if not null and equals
  end -- end for each item in the list

  GP=E and trace("[EXIT]<%s:%s> Result: Position(%d)", MOD, meth, position );
  return position;
end -- searchList()

-- ======================================================================
-- setupLdtBin()
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, userModule ) 
  local meth = "setupLdtBin()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)",MOD,meth,tostring(ldtBinName));

  local ldtCtrl = initializeLdtCtrl( topRec, ldtBinName );
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  
  -- Set the type of this record to LDT (it might already be set)
  record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in settings that override the defaults
  -- (the userModule), then process that now.
  if( userModule ~= nil )then
    local createSpecType = type(userModule);
    if( createSpecType == "string" ) then
      processModule( ldtCtrl, userModule );
    elseif( createSpecType == "userdata" ) then
      adjustLdtMap( ldtCtrl, userModule );
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

  -- NOTE: The Caller will write out the LDT bin.
  return 0;
end -- setupLdtBin( topRec, ldtBinName ) 

-- ======================================================================
-- local  CRC32 = require('CRC32'); Do this above, in the "global" area
-- ======================================================================
-- Return the hash of "value", with modulo.
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- ======================================================================
local function stringHash( value, modulo )
  local meth = "stringHash()";
  GP=E and info("[ENTER]<%s:%s> val(%s) Mod = %s", MOD, meth,
    tostring(value), tostring(modulo));

  -- local CRC32 = require('ldt/CRC32');
  if value ~= nil and type(value) == "string" then
    return CRC32.Hash( value ) % modulo;
  else
    return 0;
  end
end -- stringHash()

-- ======================================================================
-- Return the hash of "value", with modulo
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- NOTE: Use a better Hash Function.
-- ======================================================================
local function numberHash( value, modulo )
  local meth = "numberHash()";
  GP=E and info("[ENTER]<%s:%s> val(%s) Mod = %s", MOD, meth,
    tostring(value), tostring(modulo));

  local result = 0;
  -- local CRC32 = require('ldt/CRC32');
  if value ~= nil and type(value) == "number" then
    -- math.randomseed( value ); return math.random( modulo );
    result = CRC32.Hash( value ) % modulo;
  end
  GP=E and info("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
  return result
end -- numberHash

-- ======================================================================
-- computeHashCell()
-- Find the right Hash Cell for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single "Compact cell".
-- Second -- use the right hash function (depending on the type).
-- Third.  Our Lists/Arrays are based on 1 (ONE), rather than 0 (ZERO), so
-- handle that HERE -- add ONE to our result.
-- ======================================================================
local function computeHashCell( newValue, ldtMap )
  local meth = "computeHashCell()";
  GP=E and info("[ENTER]: <%s:%s> val(%s) type = %s Map(%s) ", MOD, meth,
    tostring(newValue), type(newValue), tostring(ldtMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local cellNumber  = 0;
  local key = 0; 
  -- We compute a hash ONLY for regular mode.  Compact mode always returns 0.
  if ldtMap[M_StoreState] ~= SS_COMPACT then
    key = newValue;

    -- We can probably merge number and string hash into ONE eventually.
    if type(key) == "number" then
      cellNumber  = numberHash( key, ldtMap[M_HashDirSize] );
    elseif type(key) == "string" then
      cellNumber  = stringHash( key, ldtMap[M_HashDirSize] );
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type %s (should be number, string or map)",
           MOD, meth, type(key) );
      error( ldte.ERR_INTERNAL );
    end
  end
  
  GP=E and info("[EXIT]<%s:%s> Val(%s) Hash Cell(%d) ", MOD, meth,
    tostring(newValue), cellNumber );

  return cellNumber + 1;
end -- computeHashCell()

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

  resultMap.NameList = ldrChunkRecord[LDR_NLIST_BIN];
  resultMap.NameListSize = list.size( resultMap.NameList );
  resultMap.ValueList = ldrChunkRecord[LDR_VLIST_BIN];
  resultMap.ValueListSize = list.size( resultMap.ValueList );

  return tostring( resultMap );
end -- ldrChunkSummary()

-- ======================================================================
-- Create and Init ESR
-- ======================================================================
-- The Existence SubRecord is the synchronization point for the lDTs that
-- have multiple records (one top rec and many children).  It's a little
-- like the baby sitter for the children -- it helps keeps track of them.
-- And, when the ESR is gone, we kill the children. (BRUA-HAHAHAH!!!)
--
-- All LDT sub-recs have a properties bin that describes the sub-rec.  This
-- bin contains a map that is "un-msg-packed" by the C code on the server
-- and read.  It must be the same for all LDT recs.
--
-- ======================================================================
local function createAndInitESR( topRec, ldtBinName)
  local meth = "createAndInitESR()";
  GP=E and info("[ENTER]: <%s:%s>", MOD, meth );

  local ldtCtrl = topRec[ldtBinName] ;
  local propMap = ldtCtrl[1]; 
  -- local ldtMap = ldtCtrl[2]; Not needed here
  
  local rc = 0;
  local esr       = aerospike:create_subrec( topRec );

  if( esr == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating ESR", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  local esrDigest = record.digest( esr );
  local topDigest = record.digest( topRec );

  local subRecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subRecCount + 1;

  local esrPropMap = map(); 
  
  esrPropMap[PM_Magic]        = MAGIC;
  esrPropMap[PM_RecType]      = RT_ESR;
  esrPropMap[PM_ParentDigest] = topDigest; -- Parent
  esrPropMap[PM_EsrDigest]    = esrDigest; -- Self
  esrPropMap[PM_SelfDigest]   = esrDigest;
  
  -- Set the record type as "ESR"
  GP=F trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esr, RT_ESR );
  GP=F traceinfo("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );
  
  esr[SUBREC_PROP_BIN] = esrPropMap;

  GP=F and info("[DEBUG]: <%s:%s> Leaving with ESR Digest(%s): EsrMap(%s)",
    MOD, meth, tostring(esrDigest), tostring( esrPropMap));

  -- no need to use updateSubrec for this, we dont need 
  -- maintain accouting for ESRs. 
  
  rc = aerospike:update_subrec( esr );
  if( rc == nil or rc == 0 ) then
    GP=F trace("DO NOT CLOSE THE ESR FOR NOW");
      -- aerospike:close_subrec( esr );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  -- update global attributes. 
  propMap[PM_EsrDigest] = esrDigest; 
  
  -- local NewldtCtrl = list();
  -- list.append( NewldtCtrl, propMap );
  -- list.append( NewldtCtrl, ldtMap );
  
  -- If the topRec already has an REC_LDT_CTRL_BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- setLdtRecordType( topRec );
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN ); -- propMap has been updated 

  -- Now that it's initialized, add the ESR to the SRC.
  -- addSubrecToContext( src, esr );
  GP=F and info("[DEBUG]<%s:%s>Validate ldtCtrl Contents(%s)",
    MOD, meth, tostring( ldtCtrl ));

  -- Probably shouldn't need to do this -- but this is just being extra
  -- conservative for the moment.
  -- Remove this when we know it's safe.
  rc = aerospike:update_subrec( esr );
  if( rc == nil or rc == 0 ) then
      aerospike:close_subrec( esr );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  return esrDigest;

end -- createAndInitESR()

-- ======================================================================
-- initializeSubRec()
-- ======================================================================
-- Set the values in the LDR sub-rec's Control Bin map. LDR Records
-- hold the actual data for the entries pointed to by the digest-list. 
-- This function represents the "type" LDR MAP -- all fields are
-- defined here.
-- This method needs to get called only in SS_REGULAR mode. This method will be
-- called everytime we create a new LDR entry pointed to by a digest-element in
-- the digest-list of LMAP : in short for every lmap_insert in SS_REGULAR mode. 
-- 
-- Here are the fields in an LDR Record:
-- (*) ldrRec[LDR_PROP_BIN]: The propery Map (defined here)
-- (*) ldrRec[LDR_CTRL_BIN]: The control Map (defined here)
-- (*) ldrRec[LDR_NLIST_BIN]: The Name Entry List (when in list mode)
-- (*) ldrRec[LDR_VLIST_BIN]: The Value Entry List (when in list mode)
-- (*) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
--
-- When we call this method, we have just created a LDT SubRecord.  Thus,
-- we must check to see if that is the FIRST one, and if so, we must also
-- create the Existence Sub-Record for this LDT.
-- ======================================================================
local function
initializeSubRec( topRec, ldtBinName, newLdrChunkRecord, ldrPropMap, ldrMap )
  local meth = "initializeSubRec()";
  GP=E and info("[ENTER]: <%s:%s> Name: TopRec: ", MOD, meth );

  local ldtCtrl = topRec[ldtBinName] ;
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  -- topRec's digest is the parent digest for this sub-rec 
  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  -- Subrec's (its own) digest is the selfDigest :)
  ldrPropMap[PM_SelfDigest]   = record.digest( newLdrChunkRecord ); 
  ldrPropMap[PM_Magic]        = MAGIC;
  ldrPropMap[PM_RecType]      = RT_SUB;
  
  --  Use Top level LMAP entry for mode and max values
  ldrMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries
  
  -- If this is the first LDR, then it's time to create an ESR for this
  -- LDT. There is one ESR created per LMAP bin, not per LDR chunk creation.
  if( propMap[PM_EsrDigest] == nil or ldrPropMap[PM_EsrDigest] == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> First ESR creation for LDT bin",MOD, meth);
    ldrPropMap[PM_EsrDigest] = createAndInitESR( topRec, ldtBinName );
  end

  -- Double checking the assignment -- this should NOT be needed, as the
  -- caller does it right after return of this function.
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;

  -- Set the type of this record to LDT (it might already be set by another
  -- LDT in this same record).
  record.set_type( newLdrChunkRecord, RT_SUB ); -- LDT Type Rec
end -- initializeSubRec()

-- ======================================================================
-- subRecCreate( src, topRec, ldtCtrl )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the LdtMap (the warm dir list), and return it.
-- In this function, we create a LDR sub-rec and init two structures: 
-- a. The property-map for the new LDR sub-rec chunk
-- b. The ctrl-map for the new LDR sub-rec chunk record
-- a & b are done in initializeSubRec()
-- Once that is done in the called-function, we then make a call to create 
-- an ESR and init that struct as well in createAndInitESR(). 
-- From the above function, we call setLdtRecordType() to do some 
-- byte-level magic on the ESR property-map structure. 
-- ======================================================================
-- !!!  IT APPEARS THAT THIS FUNCTION IS NOT CURRENTLY USED!!! (tjl 3/2014)
-- ======================================================================
local function subRecCreate( src, topRec, ldtBinName )
  local meth = "subRecCreate()";

  GP=E and info("[ENTER]<%s:%s> Bin(%s)", MOD, meth, tostring(ldtBinName) );
  
  -- TODO : we need to add a check to even see if we can accomodate any more 
  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.

  local newSubRec = aerospike:create_subrec( topRec );
  
  if newSubRec == nil then 
    warn("[ERROR]<%s:%s>Problems Creating Subrec New-entry(%s)",
      MOD,meth,tostring(newSubRec));
    error( ldte.ERR_SUBREC_CREATE );
  end
  
  local ldtCtrl = topRec[ldtBinName] ;
  local ldrPropMap = map();
  local ldrMap = map();
  local newChunkDigest = record.digest( newSubRec );
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  -- Update the sub-rec count (and remember to save the change)
  local subRecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subRecCount + 1;
  local rc = addSubrecToContext( src, newSubRec ); 
  
  -- Each sub-rec that gets created, needs to have its properties initialized. 
  -- Also the ESR structure needs to get created, if needed
  -- Plus the REC_LDT_CTRL_BIN of topRec needs to be updated. 
  -- This function takes care of doing all of that. 
  
  initializeSubRec( topRec, ldtBinName, newSubRec, ldrPropMap, ldrMap );

  -- Assign Prop, Control info and List info to the LDR bins
  newSubRec[SUBREC_PROP_BIN] = ldrPropMap;
  newSubRec[LDR_CTRL_BIN] = ldrMap;
  newSubRec[LDR_NLIST_BIN] = list();
  newSubRec[LDR_VLIST_BIN] = list();

  GP=E and info("[DEBUG]<%s:%s> ldrPropMap(%s) Name-list(%s) value-list(%s)",
    MOD, meth, tostring( ldrPropMap ), tostring(newSubRec[LDR_NLIST_BIN]),
    tostring(newSubRec[LDR_VLIST_BIN]));

  GP=F and info("[DEBUG]<%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrPropMap) );
  
  -- Add our new chunk (the digest) to the DigestList
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
   GP=F and trace("[DEBUG]<%s:%s> Append NewChunk(%s) Dig(%s) to DigList(%s)",
    MOD, meth, tostring(newSubRec), tostring(newChunkDigest),
    tostring(ldtMap[M_HashDirectory]));

  GP=F and trace("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LMap(%s): ",
    MOD, meth, tostring(newChunkDigest), tostring(ldtMap));
   
  -- Increment the Digest Count
  -- gets inceremented once per LDR entry add. 
  local ChunkCount = ldtMap[M_ListDigestCount]; 
  ldtMap[M_ListDigestCount] = (ChunkCount + 1);

  -- This doesn't appear to be getting set (updated) anywhere else.
  -- Do it here.
  aerospike:update_subrec( newSubRec );

  GP=E and info("[EXIT]: <%s:%s> ldrPropMap(%s) Name-list: %s value-list: %s ",
    MOD, meth, tostring( ldrPropMap ), tostring(newSubRec[LDR_NLIST_BIN]),
    tostring(newSubRec[LDR_VLIST_BIN]));
  
  return newSubRec;
end --  subRecCreate()

-- =======================================================================
-- searchNameList()
-- =======================================================================
-- Search a list for an item.  Similar to LSET searchNameList(), but for MAP
-- we are searching just the NAME list, which is always atomic.
--
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) nameList: the list of values from the record
-- (*) searchKey: the atomic value that we're searching for.
-- Return the position if found, else return ZERO.
-- =======================================================================
local function searchNameList(ldtCtrl, nameList, searchKey )
  local meth = "searchNameList()";
  GP=E and trace("[ENTER]: <%s:%s> Looking for searchKey(%s) in List(%s)",
     MOD, meth, tostring(searchKey), tostring(nameList));
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local position = 0; 

  -- Nothing to search if the list is null or empty
  if( nameList == nil or list.size( nameList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyList", MOD, meth );
    return 0;
  end

  -- Search the list for the item (searchKey) return the position if found.
  -- Note that searchKey may be the entire object, or it may be a subset.
  local listSize = list.size(nameList);
  local item;
  local dbKey;
  for i = 1, listSize, 1 do
    item = nameList[i];
    GP=F and trace("[COMPARE]<%s:%s> index(%d) SV(%s) and ListVal(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(item));
    -- a value that does not exist, will have a nil nameList item
    -- so we'll skip this if-loop for it completely                  
    if item ~= nil and item == searchKey then
      position = i;
      break;
    end -- end if not null and not empty
  end -- end for each item in the list

  GP=E and trace("[EXIT]<%s:%s> Result: Position(%d)", MOD, meth, position );
  return position;
end -- searchNameList()

-- ======================================================================
-- ldrInsertList( topLdrChunk, ldtCtrl, listIndex, nameList, valueList )
-- ======================================================================
-- Insert (append) the LIST of values pointed to from the digest-list, 
-- to this chunk's value list.  We start at the position "listIndex"
-- in nameList/ValueList.  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in the lists from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the Ldt control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) nameList:
-- (*) valueList
-- Return: Number of items written
-- ======================================================================
local function ldrInsertList(ldrChunkRec,ldtCtrl,listIndex,nameList,valueList )
  local meth = "ldrInsertList()";
  GP=E and info("[ENTER]: <%s:%s> Index(%d) nameList(%s) valueList(%s)",
    MOD, meth, listIndex, tostring( nameList ), tostring( valueList ));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
 
   if ldrChunkRec == nil then
 	-- sanity check 
    warn("[ERROR]: <%s:%s>: ldrChunkRec nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
  else
  	GP=F and info(" LDRCHUNKREC not nil <%s:%s>  ", MOD, meth);
  end

  -- These 2 get assigned in subRecCreate() to point to the ctrl-map. 
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and info(" <%s:%s> Chunk ldrMap is [DEBUG] (%s)",
    MOD, meth, tostring(ldrMap));
  
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];
    
   GP=F and info(" <%s:%s> Chunk ldr Name-List: %s Value-List: (%s)",
     MOD, meth, tostring(ldrNameList), tostring(ldrValueList));
   GP=F and info(" <%s:%s> To be inserted Name-List: %s Value-List: (%s)",
     MOD, meth, tostring(nameList), tostring(valueList));
  
  local chunkNameIndexStart = list.size( ldrNameList ) + 1;
  local chunkValueIndexStart = list.size( ldrValueList ) + 1;
  
  GP=F and info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( nameList ) + 1 - listIndex;
  local itemSlotsAvailable =
      (ldtMap[M_LdrEntryCountMax] - chunkNameIndexStart) + 1;

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
    ldtMap[M_TopFull] = true;
    GP=F and info("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):WTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and info("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.
  -- GP=F and info("[DEBUG]<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
  --  MOD, meth, listIndex, chunkIndexStart, newItemsStored );
    
  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrNameList, nameList[i+listIndex] );
    list.append( ldrValueList, valueList[i+listIndex] );
  end -- for each remaining entry

  GP=F and info("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec[LDR_CTRL_BIN] = ldrMap;
  ldrChunkRec[LDR_NLIST_BIN] = ldrNameList;
  ldrChunkRec[LDR_VLIST_BIN] = ldrValueList;
   
  GP=E and info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()

-- ======================================================================
-- ldrInsertBytes( topLdrChunk, ldtCtrl, listIndex, nameList, valueList )
-- ======================================================================
-- Insert (append) the LIST of values pointed to from the digest-list, 
-- to this chunk's Byte Array.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- This method is similar to its sibling "ldrInsertList()", but rather
-- than add to the entry list in the chunk's LDR_LIST_BIN, it adds to the
-- byte array in the chunk's LDR_BNRY_BIN.
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the LMAP control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) nameList:
-- (*) valueList:
-- Return: Number of items written
-- ======================================================================
local function
ldrInsertBytes( ldrChunkRec, ldtCtrl, listIndex, nameList, valueList )
  local meth = "ldrInsertBytes()";
  GP=E and info("[ENTER]: <%s:%s> Index(%d) NameList(%s) ValueList(%s)",
    MOD, meth, listIndex, tostring(nameList), tostring(valueList));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and info("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = ldtMap[M_LdrByteEntrySize];
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", MOD, meth);
    -- Let the caller handle the error.
    error( ldte.ERR_INTERNAL );
  end

  local entryCount = 0;
  if( ldrMap[LDR_ByteEntryCount] ~= nil and ldrMap[LDR_ByteEntryCount] ~= 0 )
  then
    entryCount = ldrMap[LDR_ByteEntryCount];
  end
  GP=F and info("[DEBUG]:<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount );

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  local totalItemsToWrite = list.size( nameList ) + 1 - listIndex;
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
    ldtMap[M_TopFull] = true; -- Remember to reset on next update.
    GP=F and info("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):WTop FULL!!",
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
    GP=F and info("[DEBUG]:<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
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

  GP=F and info("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d) ByteStart(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable, chunkByteStart );

  local byteIndex;
  local insertItem;
  for i = 0, (newItemsStored - 1), 1 do
    byteIndex = chunkByteStart + (i * entrySize);
    insertItem = valueList[i+listIndex];

    GP=F and
    trace("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
      MOD, meth, tostring( chunkByteArray), i, tostring( insertItem ),
      byteIndex );

    bytes.put_bytes( chunkByteArray, byteIndex, insertItem );

    GP=F and info("[DEBUG]: <%s:%s> Post Append: ByteArray(%s)",
      MOD, meth, tostring(chunkByteArray));
  end -- for each remaining entry

  -- Update the ctrl map with the new count
  ldrMap[LDR_ByteEntryCount] = entryCount + newItemsStored;

  GP=F and info("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring( chunkByteArray ));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec[LDR_CTRL_BIN] = ldrMap;
  ldrChunkRec[LDR_BNRY_BIN] = chunkByteArray;

  GP=E and info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()

-- ======================================================================
-- ldrInsert()
-- ======================================================================
-- Insert (append) the LIST of values to the digest-list created for LMAP. 
-- !!!!!    This is applicable only in SS_REGULAR mode !!!!!!!!!!!!!!!!!!!
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in SM_LIST mode or
-- SM_BINARY mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the LMAP control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsert(ldrChunkRec,ldtCtrl,listSize, nameList, valueList )
  local meth = "ldrInsert()";

  GP=E and trace("[ENTER]<%s:%s> ListSz(%d) NameList(%s), valueList(%s)",
    MOD, meth, listSize, tostring( nameList ), tostring( valueList ));

  GP=E and trace("[DEBUG]<%s:%s> ChunkSummary(%s)", 
    MOD, meth, tostring(ldrChunkRec));
    
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  if ldtMap[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,ldtCtrl,listSize,nameList,valueList);
  else
    return ldrInsertBytes(ldrChunkRec,ldtCtrl,listSize,nameList,valueList);
  end

end -- ldrInsert()

-- ========================================================================
-- lmapGetLdrDigestEntry()
-- ========================================================================
-- Get a Digest entry 
-- !!! THIS CODE NEEDS REVIEW !!!
-- ========================================================================
local function
lmapGetLdrDigestEntry( src, topRec, ldtBinName, entryItem, create_flag)
  local meth = "lmapGetLdrDigestEntry()";
  
  local ldtCtrl = topRec[ldtBinName] ;
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local topLdrChunk = nil; 

  GP=E and info("[ENTER]: <%s:%s> lMap(%s)", MOD, meth, tostring( ldtMap ));
  
  local cellNumber = computeHashCell( entryItem, ldtMap ); 
  local digestlist = ldtMap[M_HashDirectory]; 
	
  GP=F and info(" <%s:%s> : Digest-entry for this index %d ",
             MOD, meth, cellNumber);
             
  if digestlist == nil then
    -- sanity check 
    warn("[ERROR]: <%s:%s>: Digest list nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
 end 
   	
  GP=F and info(" <%s:%s> !!!!!!! Digest List size : %d list %s", MOD, meth,
    list.size( digestlist ), tostring(digestlist));
   	
  local newdigest_list = list(); 
  for i = 1, list.size( digestlist ), 1 do
     if i == cellNumber then 
	    
       if digestlist[i] == 0 then 
         -- This is a new unique key, create a chunk  
         GP=F and info(" <%s:%s> : Digest-entry empty for this index %d ",
         MOD, meth, cellNumber);
         GP=F and info("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
         topLdrChunk = subRecCreate( src, topRec, ldtBinName ); -- create new
         ldtMap[M_TopFull] = false; -- reset for next time.
         create_flag = true; 
          
       else 
          -- local newChunkDigest = record.digest( topLdrChunk );
          GP=F and info("[DEBUG]<%s:%s> Digest-entry valid: index %d digest(%s)",
            MOD, meth, cellNumber, tostring( digestlist[i] ));
          local stringDigest = tostring( digestlist[i] );
          topLdrChunk = openSubrec( src, topRec, stringDigest );
       end
          
     end -- end of digest-bin if, no concept of else, bcos this is a hash :)

  end -- end of for 
  
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth ); 
  return topLdrChunk; 

end --function lmapGetLdrDigestEntry()

-- ========================================================================
-- lmapCheckDuplicate()
-- ========================================================================
local function lmapCheckDuplicate(ldtMap, ldrChunkRec, entryItem)
  local meth = "lmapCheckDuplicate()";
  
  local flag = false; 
  if ldtMap[M_StoreMode] == SM_LIST then
    local ldrValueList = ldrChunkRec[LDR_NLIST_BIN];
    GP=F and trace("[DEBUG]<%s:%s> Duplicate check list %s", MOD, meth,
      tostring(ldrValueList));
    for i = 1, list.size( ldrValueList ), 1 do
    	if ldrValueList[i] == entryItem then 
    		flag = true; 
    		GP=F and info(" Entry already Exists !!!!!"); 
    		return flag; 
    	end -- end of if check 
     end -- end of for loop for list 
  end -- list check 
  
  -- TODO : No code yet for duplicate checking in byte-mode
  
  return flag; 
end -- function lmapCheckDuplicate()

-- ======================================================================
-- compactInsert( ldtCtrl, newName, newValue );
-- ======================================================================
-- Search the compact list, and insert if not found.
-- Parms:
-- (*) ldtCtrl: The main LDT Structure
-- (*) newName: Name to be inserted
-- (*) newValue: Value to be inserted
-- ======================================================================
local function compactInsert( ldtCtrl, newName, newValue )
  local meth = "compactInsert()";
  GP=E and info("[ENTER]<%s:%s>Insert Name(%s) Value(%s)",
    MOD, meth, tostring(newName), tostring(newValue));
  
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  
  -- NOTE: We're expecting the lists to be built, and it's an error if
  -- they are not there.
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 

  if nameList == nil or valueList == nil then
    warn("[ERROR]:<%s:%s> Name/Value is nil: name(%s) value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
    error( ldte.ERR_INTERNAL );
  end

  local position = searchList( ldtCtrl, nameList, newName );
  if( position > 0 and ldtMap[M_OverWrite] == AS_FALSE) then
    info("[UNIQUE VIOLATION]:<%s:%s> Name(%s) Value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
    error( ldte.ERR_INTERNAL );
  end

  -- Store the name in the name list.  If we're doing transforms, do that on
  -- the value and then store it in the valueList.
  list.append( nameList, newName );
  local storeValue = newValue;
  if( G_Transform ~= nil ) then
    storeValue = G_Transform( newValue );
  end
  list.append( valueList, storeValue );

  GP=E and trace("[EXIT]<%s:%s>Name(%s) Value(%s) NameList(%s) ValList(%s)",
     MOD, meth, tostring(newName), tostring(newValue), 
     tostring(nameList), tostring(valueList));
  -- No need to return anything
end -- compactInsert()

-- ======================================================================
-- Create a new Sub-Record and initialize it.
-- Parms:
-- (*) src: subrecContext: The pool of open sub-records
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtCtrl: Main LDT Control Structure
-- Contents of a Sub-Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) LDR_CTRL_BIN:    Main Node Control structure
-- (3) LDR_NLIST_BIN:   The Name List
-- (4) LDR_VLIST_BIN:   The Value List
-- (5) LDR_BINARY_BIN:  Packed Binary Array of values(if used) goes here
-- ======================================================================
local function createSubRec( src, topRec, ldtCtrl )
  local meth = "createSubRec()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];

  -- Create the SubRec, and remember to add this to the SRC
  local subRec = aerospike:create_subrec( topRec );
  if( subRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating Subrec", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end
  addSubrecToContext( src, subRec );

  local rc = initializeNode( topRec, subRec, ldtCtrl );
  if( rc >= 0 ) then
    GP=F and trace("[DEBUG]<%s:%s>Node Init OK", MOD, meth );
    rc = aerospike:update_subrec( subRec );
  else
    warn("[ERROR]<%s:%s> Problems initializing Node(%d)", MOD, meth, rc );
    error( ldte.ERR_INTERNAL );
  end

  -- Must wait until subRec is initialized before it can be added to SRC.
  -- It should be ready now.
  addSubrecToContext( src, subRec );

  GP=E and trace("[EXIT]<%s:%s> rc(%s)", MOD, meth, tostring(rc) );
  return subRec;
end -- createSubRec()


-- ======================================================================
-- Hash Directory Management
-- ======================================================================
-- Using the Linear Hash Algorithm, we will incrementally expand the
-- hash directory.  With Linear Hash, two things happen.   There is the
-- physical directory change, and then there is the logical address change.
--
--  +----+----+
--  |Cell|Cell|
--  | 1  | 2  |
--  +----+----+
--  +----+----+----+----+
--  |Cell|Cell|Cell|Cell|   
--  | 1  | 2  | 3  | 4  |   
--  +----+----+----+----+
--  +----+----+----+----+----+----+----+----+
--  |Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell|   
--  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  |   
--  +----+----+----+----+----+----+----+----+
-- ======================================================================
-- ======================================================================

-- ======================================================================
-- hashCellListInsert()
-- ======================================================================
-- Insert into a hash cell list.  Easy Peasy.
-- ======================================================================
local function hashCellListInsert( cellAnchor, newName, newValue )
  local meth = "hashCellListInsert()";
  GP=E and trace("[ENTER]<%s:%s> newName(%s) newValue(%s)", MOD, meth,
    tostring(newName), tostring(newValue));

  cellAnchor[C_CellState] = C_STATE_LIST;
  local nameList  = cellAnchor[C_CellNameList];
  local valueList = cellAnchor[C_CellValueList];
   
  -- Add the new name/value to the existing list and then assign the lists
  -- to the sub-record.
  list.append( nameList, newName );

  -- If we have a transform to perform, do that now and then store the value
  local storeValue = newValue;
  if( G_Transform ~= nil ) then
    storeValue = G_Transform( newValue );
  end
  list.append( valueList, storeValue );

 end -- function hashCellListInsert()

-- ======================================================================
-- hashCellConvertInsert()
-- ======================================================================
-- Convert the hash cell LIST into a single sub-record, change the cell
-- anchor state (to DIGEST) and then move the list data into the Sub-Rec.
-- ======================================================================
local function
hashCellConvertInsert(src, topRec, ldtCtrl, cellAnchor, newName, newValue)
  local meth = "hashCellConvertInsert()";
  GP=E and trace("[ENTER]<%s:%s> HCell(%s) newName(%s) newValue(%s)", MOD, meth,
    tostring(cellAnchor), tostring(newName), tostring(newValue));

  -- Validate that the state of this Hash Cell Anchor really is "LIST",
  -- because otherwise something bad would be done.
  if( cellAnchor[C_CellState] ~= C_STATE_LIST ) then
    warn("[ERROR]<%s:%s> Bad Hash Cell Anchor State(%s). Should be LIST.",
      MOD, meth, tostring(cellAnchor[C_CellState]));
    error( ldte.ERR_INTERNAL );
  end

  -- Create a new Sub-Rec, store the digest and store the list data. 
  -- Note that we don't need to check values or counts, because we already
  -- know that we're good.   We are assuming that no single value is
  -- so ungodly large that we can get in trouble with moving a small list
  -- into a Sub-Rec.  If that DOES get us into trouble, then we have to
  -- figure out better INTERNAL support for checking sizes of Lua objects.
  local subRec = createSubRec( src, topRec, ldtCtrl );
  local digest = record.digest( subRec );

  if( subRec == nil ) then
    warn("[ERROR]<%s:%s>: SubRec Create Error",  MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  local nameList = cellAnchor[C_CellNameList];
  local valueList = cellAnchor[C_CellValueList];

  if( nameList == nil or valueList == nil ) then
    warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  -- Make sure the new name is NOT in the existing nameList.  If so, 
  -- then ERROR if "Overwrite" is not turned on.
  local position = searchList( ldtCtrl, nameList, newName );
  if( position > 0 and ldtMap[M_OverWrite] == AS_FALSE) then
    info("[UNIQUE VIOLATION]:<%s:%s> Name(%s) Value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
    error( ldte.ERR_INTERNAL );
  end

  -- Add the new name/value to the existing list and then assign the lists
  -- to the sub-record.
  list.append( nameList, newName );
  -- If we have a transform to perform, do that now and then store the value
  local storeValue = newValue;
  if( G_Transform ~= nil ) then
    storeValue = G_Transform( newValue );
  end
  list.append( valueList, storeValue );
  
  subRec[LDR_NLIST_BIN] = nameList;
  subRec[LDR_VLIST_BIN] = valueList;

  -- Set the state the hash cell to "DIGEST" and then NULL out the list
  -- values (which are now in the sub-rec).
  cellAnchor[C_CellState] = C_STATE_DIGEST;
  cellAnchor[C_CellNameList] = nil;
  cellAnchor[C_CellValueList] = nil;

  GP=E and info("[EXIT]<%s:%s> Conversion Successful", MOD, meth );

end -- function hashCellConvertInsert()

-- ======================================================================
-- hashCellSubRecInsert()
-- ======================================================================
-- Insert the value into the sub-record.  If this insert would trigger
-- an overflow, then split the sub-record into two.
-- This might be an existing radix tree with sub-recs, or it might be
-- a single sub-rec that needs to split and introduce a tree.
-- ======================================================================
local function
hashCellSubRecInsert(src, topRec, ldtCtrl, cellAnchor, newName, newValue)
  -- LMAP Version 1:  Just a pure Sub-Rec insert, no trees just yet.
  local digest = cellAnchor[C_CellDigest];
  local digestString = tostring(digest);
  local subRec = openSubrec( src, topRec, digestString );

  if( subRec == nil ) then
    warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",  MOD, meth,
      digestString );
    error( ldte.ERR_SUBREC_OPEN );
  end

  -- ATTENTION!!!  Here is the place where we will eventually do the check
  -- for single Sub-Rec overflow and turn the single sub-rec into a Radix
  -- tree of multiple sub-records.

  local nameList = subRec[LDR_NLIST_BIN];
  local valueList = subRec[LDR_VLIST_BIN];
  if( nameList == nil or valueList == nil ) then
    warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  local position = searchList( ldtCtrl, nameList, newName );
  if( position > 0 and ldtMap[M_OverWrite] == AS_FALSE) then
    info("[UNIQUE VIOLATION]:<%s:%s> Name(%s) Value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
    error( ldte.ERR_INTERNAL );
  end

  list.append( nameList, newName );
  -- If we have a transform to perform, do that now and then store the value
  local storeValue = newValue;
  if( G_Transform ~= nil ) then
    storeValue = G_Transform( newValue );
  end
  list.append( valueList, storeValue );

  GP=E and info("[EXIT]<%s:%s> SubRecInsert Successful", MOD, meth );
end -- function hashCellSubRecInsert()


-- ======================================================================
-- regularInsert()
-- ======================================================================
-- Perform a "Regular" Hash Directory Insert.
-- First, Take the new value and locate the Hash Directory Cell.
-- Then, look inside the Cell:  There will be a Cell Anchor that might
-- hold a LIST, a single SubRec Pointer (digest) or a Radix Tree of multiple
-- Digests.
-- Locate the correct spot (either immediate list or sub-record).
-- Insert the sub-record into the spot.
-- Additional Complications:
-- (*) We've hit an overflow situation:
--   (1) We must either convert the list into a Sub-Record
--   (2) We must convert a single Sub-Record into a Radix Tree
--   (3) We must split the Sub-Record of a tree into two Sub-Records.
-- Step ONE: Use a Single Sub-Record.  Note the state so that we can
-- gracefully extend into use of a Radix Tree.
-- ======================================================================
local function regularInsert( src, topRec, ldtCtrl, newName, newValue )
  local meth = "regularInsert()";
  GP=E and trace("[ENTER]<%s:%s> Name(%s) Value(%s)",
   MOD, meth, tostring(newName), tostring(newValue));
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local rc = 0; -- start out OK.

  local cellNumber = computeHashCell( newName, ldtMap );
  -- Remember that our Hash Dir goes from 1..N, rather than 0..(N-1)
  local hashDirectory = ldtMap[M_HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  GP=F and trace("[DEBUG]<%s:%s> CellNum(%s) CellAnchor(%s)", MOD, meth,
    tostring(cellNumber), tostring(cellAnchor));

  -- Maybe, eventually, we'll allow a few items to be stored directly
  -- in this directory (to save the SUBREC management for small numbers).
  -- TODO: Add ability to hold small lists -- and read them -- soon.

  -- If no hash cell anchor is present, then we're in trouble.  There should
  -- ALWAYS be a cell anchor, even when no data is there.
  if( cellAnchor == nil or cellAnchor == 0 ) then
    GP=F and warn("[ERROR]<%s:%s> MISSING HASH CELL ANCHOR for Cell(%s)",
      MOD, meth, tostring(cellAnchor));
    error(ldte.ERR_INTERNAL);
  end

  -- We have three main cases:
  -- (1) Empty Hash Cell.  We allocate a list and append it.
  -- (2) List Hash Cell.  We check for overflow:
  --     If it fits, we append to list.
  --     Else, we convert this Cell into a subRec and insert.
  -- (3) It's a sub-rec cell (or a tree cell).
  --     Do a sub-rec insert.
  if ( cellAnchor[C_CellState] == C_STATE_EMPTY ) then
    -- Easy :: hash cell list insert.
    cellAnchor[C_CellNameList] = list();
    cellAnchor[C_CellValueList] = list();
    hashCellListInsert( cellAnchor, newName, newValue );
  elseif ( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- We have a list.  See if we're already at the threshold.
    local listSize = list.size( cellAnchor[C_CellNameList] );
    if ( listSize < ldtMap[M_HashCellMaxList] ) then
      -- Still easy.  List insert.
      hashCellListInsert( cellAnchor, newName, newValue );
    else
      -- Harder.  Convert List into Subrec and insert.
      hashCellConvertInsert(src, topRec, ldtCtrl, cellAnchor, newName, newValue);
    end
  else
    -- It's a sub-record insert, with a possible tree overflow
      hashCellSubRecInsert(src, topRec, ldtCtrl, newName, newValue);
  end

  GP=E and info("[EXIT]<%s:%s> SubRecInsert Successful", MOD, meth );
end -- function regularInsert()

-- ======================================================================
-- listDelete()
-- ======================================================================
-- General List Delete function that can be used to delete items, employees
-- or pesky Indian Developers (usually named "Raj").
-- RETURN:
-- A NEW LIST that no longer includes the deleted item.
-- ======================================================================
local function listDelete( objectList, position )
  local meth = "listDelete()";
  local resultList;
  local listSize = list.size( objectList );

  GP=E and trace("[ENTER]<%s:%s>List(%s) size(%d) Position(%d)", MOD,
  meth, tostring(objectList), listSize, position );
  
  if( position < 1 or position > listSize ) then
    warn("[DELETE ERROR]<%s:%s> Bad position(%d) for delete.",
      MOD, meth, position );
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
  --
  -- An alternative method would be to swap the current position with
  -- the END value, and then perform a "trim" on the list -- if that
  -- actually worked.
  --
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

  GP=F and trace("[EXIT]<%s:%s>List(%s)", MOD, meth, tostring(resultList));
  return resultList;
end -- listDelete()

-- ======================================================================
-- || validateValue()
-- ======================================================================
-- In the calling function, we've landed on the name we were looking for,
-- but now we have to potentially untransform and filter the value -- so we
-- do that here.
-- ======================================================================
local function validateValue( storedValue )
  local meth = "validateValue()";

  GP=E and trace("[ENTER]<%s:%s> validateValue(%s)",
                 MOD, meth, tostring( storedValue ) );
                 
  local liveObject;
  -- Apply the Transform (if needed), as well as the filter (if present)
  if( G_UnTransform ~= nil ) then
    liveObject = G_UnTransform( storedValue );
  else
    liveObject = storedValue;
  end
  -- If we have a filter, apply that.
  if( G_Filter ~= nil ) then
    resultFiltered = G_Filter( liveObject, G_FunctionArgs );
  else
    resultFiltered = liveObject;
  end
  return resultFiltered; -- nil or not, we just return
end -- validateValue()

-- ======================================================================
-- compactDelete()
-- ======================================================================
-- Delete an item from the compact list.
-- For the compact list, it's a simple list delete (if we find it).
-- (*) topRec: The Aerospike record holding the LDT
-- (*) ldtCtrl: The main LDT control structure
-- (*) searchName: the name of the name/value pair to be deleted
-- (*) resultMap: the map carrying the name/value pair result.
-- ======================================================================
local function compactDelete( ldtCtrl, searchName, resultMap )
  local meth = "compactDelete()";

  GP=E and trace("[ENTER]<%s:%s> Name(%s)", MOD, meth, tostring(searchName));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local nameList = ldtMap[M_CompactNameList];
  local valueList = ldtMap[M_CompactValueList];

  local position = searchList( ldtCtrl, nameList, searchName );
  if( position == 0 ) then
    -- Didn't find it -- report an error.
    warn("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok -- found the name, so let's delete the value.
  -- listDelete() will generate a new list, so we store that back into
  -- the ldtMap.
  resultMap[searchName] = validateValue( valueList[position] );
  ldtMap[M_CompactNameList]  = listDelete( nameList, position );
  ldtMap[M_CompactValueList] = listDelete( valueList, position );

  listDelete( nameList, position );
  listDelete( valueList, position );

  GP=E and debug("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- compactDelete()

-- ======================================================================
-- subRecDelete()
-- ======================================================================
-- Remove a map entry from a SubRec (regular storage mode).
-- Params:
-- (*) topRec: The Aerospike record holding the LDT
-- (*) ldtCtrl: The main LDT control structure
-- (*) searchName: the name of the name/value pair to be deleted
-- (*) resultMap: the map carrying the name/value pair result.
-- ======================================================================
local function subRecDelete( topRec, ldtCtrl, searchName, resultMap )
  local meth = "subRecDelete()";
  GP=E and trace("[ENTER]<%s:%s> Name(%s)", MOD, meth, tostring(searchName));

  local rc = 0; -- start out OK.
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  -- Compute the subRec address that holds the searchName
  local cellNumber = computeHashCell( searchName, ldtMap );
  local hashDirectory = ldtMap[M_HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];
  local subRec;
  local src = createSubrecContext();

  -- If no sub-record, then not found.
  if( cellAnchor == nil or cellAnchor == 0 ) then
    warn("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- We have a sub-rec -- open it
  local digest = cellAnchor[C_CellDigest];
  if( digest == nil ) then
    warn("[ERROR]: <%s:%s>: nil Digest value",  MOD, meth );
    error( ldte.ERR_SUBREC_OPEN );
  end

  local digestString = tostring(digest);
  local subRec = openSubrec( src, topRec, digestString );
  if( subRec == nil ) then
    warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",  MOD, meth,
      digestString );
    error( ldte.ERR_SUBREC_OPEN );
  end

  local nameList = subRec[LDR_NLIST_BIN];
  local valueList = subRec[LDR_VLIST_BIN];
  if( nameList == nil or valueList == nil ) then
    warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  local position = searchList( ldtCtrl, nameList, searchName );
  if( position == 0 ) then
    -- Didn't find it -- report an error.
    -- First -- Close the subRec.
    aerospike:close_subrec( subRec );

    warn("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok -- found the name, so let's delete the value.
  -- listDelete() will generate a new list, so we store that back into
  -- the Sub-Record.
  resultMap[searchName] = validateValue( valueList[position] );
  subRec[LDR_NLIST_BIN] = listDelete( nameList, position );
  subRec[LDR_VLIST_BIN] = listDelete( valueList, position );

  GP=E and debug("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- function subRecDelete()

-- ======================================================================
-- ldrDeleteList()
-- ======================================================================
-- Insert (append) the LIST of values pointed to from the digest-list, 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the LDT control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) entryList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================

local function
ldrDeleteList(topRec, ldtBinName, ldrChunkRec, listIndex, entryList )
  local meth = "ldrDeleteList()";

  GP=E and info("[ENTER]: <%s:%s> Index(%d) Search-List(%s)",
    MOD, meth, listIndex, tostring( entryList ) );

  local ldtCtrl = topRec[ldtBinName]; 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local self_digest = record.digest( ldrChunkRec ); 

  -- Set up the Read/Write Functions (KeyFunction, Transform, Untransform)
  setReadFunctions( ldtMap, nil, nil, nil );

  -- These 2 get assigned in subRecCreate() to point to the ctrl-map. 
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];

  if ldrNameList == nil then 
    -- Nothing to be deleted in this subRec
    GP=E and info("[ENTER]: <%s:%s> Nothing to be deleted in this subRec !!",
    MOD, meth );
    return -1; 
  end
 
  GP=F and info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToDelete = list.size( entryList );
  local totalListSize = list.size( ldrNameList );
  
  GP=F and info("[DEBUG]: <%s:%s> TotalItemsToDelete(%d) ListSize(%d)",
    MOD, meth, totalItemsToDelete, totalListSize );
    
  if totalListSize < totalItemsToDelete then
    warn("[INTERNAL ERROR]<%s:%s> LDR list is shorter than deletion list(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end
 
  -- Basically, crawl thru the list, copy-over all except our item to the
  -- new list re-append back to ldrmap. Easy !
  
  GP=F and info("\n[DEBUG]<%s:%s>:ListMode: Before deletion Value List %s",
     MOD, meth, tostring( ldrValueList ) );
 
  local NewldrNameList = list(); 
  local NewldrValueList = list(); 
  local num_deleted = 0; 
  GP=F and info("[DEBUG]<%s> Before Delete Name(%s) Value(%s)", meth,
    tostring(ldrNameList), tostring(ldrValueList));
  for i = 0, list.size( ldrNameList ), 1 do
    -- If the search-name in vame-value pair matches any-name in the chunk entry 
    -- then pick out the corresponding value-entry and nil them out.
    -- AS OF NOW, WE ALWAYS SEND ONLY ONE INDEX-ENTRY TO BE SEARCHED 
    if(tostring(ldrNameList[i]) ~= tostring(entryList[1])) then
      list.append(NewldrNameList, ldrNameList[i]);  
      list.append(NewldrValueList, ldrValueList[i]);  
    end
  end
  ldrChunkRec[LDR_NLIST_BIN] = NewldrNameList; 
  ldrChunkRec[LDR_VLIST_BIN] = NewldrValueList; 
  GP=F and info("[DEBUG]<%s:%s> AfterDelete Name & Value %s %s", MOD, meth,
    tostring(ldrNameList), tostring(ldrValueList));
 
  -- Update subRec 
  local rc = aerospike:update_subrec( ldrChunkRec );
  if( rc == nil or rc == 0 ) then
      -- Close ALL of the subRecs that might have been opened
      GP=F and info("[DEBUG]<%s:%s> SUB-REC  Update Status(%s) ", MOD, meth,
        tostring(rc));
  else
     warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
     error( ldte.ERR_SUBREC_UPDATE );
  end

  local num_deleted = totalListSize - list.size( ldrChunkRec[LDR_NLIST_BIN] ); 
  GP=F and info(" Delete : Num-deleted :%s", tostring(num_deleted));  
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount - num_deleted; -- # of valid items goes down
  ldtMap[M_TotalCount] = totalCount - num_deleted; -- Total # of items goes down
  
  GP=F and info(" Delete : Num-deleted :%s Mapcount %s", tostring(num_deleted), tostring(propMap[PM_ItemCount])); 
 
  -- Now go and fix the digest-list IF NEEDED 
  -- refer to lmap_design.lua to determine what needs to be done here.
  -- we deleted the one and only (or last) item in the LDR list. 
  if totalListSize == totalItemsToDelete and
    list.size( ldrChunkRec[LDR_NLIST_BIN] ) == 0
  then
    GP=F and info("[DEBUG] !!!!!!!!! Entire LDR list getting Deleted !!!!!!");
    local digestlist = ldtMap[M_HashDirectory]; 
    GP=F and info("[DEBUG}<%s:%s> Digest %s to List we are comapring with %s",
    MOD, meth, tostring(self_digest), tostring(digestlist));
    for i = 1, list.size( digestlist ), 1 do
      if tostring(digestlist[i]) == tostring(self_digest) then 
        GP=F and info("[FOUND MATCH] digest-list Delete Index %d", i);
   	GP=F and info("[DEBUG] List BEFORE reset Delete: %s", tostring(digestlist))
        GP=F and info("[DEBUG] !! Resetting Delete digest-entry %s to zero !!",
   		         tostring( digestlist[i] ) );
   	digestlist[i] = 0; 
   	GP=F and info("[DEBUG]List AFTER Delete reset : %s", tostring(digestlist))
      end 
    end -- end of for loop 
  
   -- update TopRec ()
   ldtMap[M_HashDirectory] = digestlist; 
   
 end -- end of if check for digestlist reset 
   
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
     GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
  else
     warn("[ERROR]<%s:%s>TopRec Update:rc(%s)",MOD,meth,tostring(rc));
     error( ldte.ERR_SUBREC_UPDATE );
  end 
   
 return num_deleted;
end -- ldrDeleteList()

-- ==========================================================================
-- ldrSearchList()
-- ==========================================================================
local function
ldrSearchList(topRec, ldtBinName, resultMap, ldrChunkRec, listIndex, entryList )
  local meth = "ldrSearchList()";
  GP=E and info("[ENTER]<%s:%s> Index(%d) List(%s)",
           MOD, meth, listIndex, tostring( entryList ));

  local ldtCtrl = topRec[ldtBinName]; 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local self_digest = record.digest( ldrChunkRec ); 

  -- These 2 get assigned in subRecCreate() to point to the ctrl-map. 
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];

  if ldrNameList == nil then 
    -- Nothing to be searched for in this sub-rec
    return -1; 
  end 

  GP=F and info("[DEBUG]<%s:%s> Chunk: CTRL(%s) NList: %s VList(%s)", MOD, meth,
    tostring( ldrMap ), tostring(ldrNameList), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  
  -- Code to return all the elements of the ldr-list array, iff 
  -- entryList size is 0 
  
  if list.size( entryList ) == 0 and ldrNameList ~= nil then 
    -- return the entire list
    GP=F and info(" Search string is NULL, returning the entire LDR list"); 
    for i = 0, list.size( ldrNameList ), 1 do
      if ldrNameList[i] ~= nil then 
        local resultFiltered = ldrValueList[i];
        if( G_Filter ~= nil ) then
          resultFiltered = G_Filter( ldrValueList[i], G_FunctionArgs );
        else
      	  resultFiltered = ldrValueList[i];
        end
        -- local newString = ldrNameList[i]..":"..resultFiltered; 
        -- list.append( resultList, newString );
        resultMap[ldrNameList[i]] = resultFiltered;
      end
    end
    return 0; 
  end 
  
  local totalItemsToSearch = list.size( entryList ) + 1 - listIndex;
  local totalListSize = list.size( ldrValueList );
  
  GP=F and info("[DEBUG]<%s:%s> TotalItems(%d) ListSize(%d) searchList(%s)",
    MOD, meth, totalItemsToSearch, totalListSize, tostring(entryList) );
    
  if totalListSize < totalItemsToSearch then
  	-- TODO : Check with Toby about this condition 
  	-- also applicable to deletes in regular mode 
    warn("[INTERNAL ERROR] <%s:%s> LDR list is shorter than Search list(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end
 
  -- Basically, crawl thru the list, copy-over all except our item to the
  -- new list re-append back to ldrmap. Easy !
  
  GP=F and info("[DEBUG]<%s:%s>:ListMode: Search target list %s ",
     MOD, meth, tostring( ldrValueList ) );
  
  -- This will also work if we search for more than 1 item in the ldr-list
  -- why exactly do we need this fancy nested for-loop ?

  for j = 0, list.size( entryList ), 1 do
    for i = 0, list.size( ldrNameList ), 1 do
      if ldrNameList[i] ~= nil then 
        if(tostring(ldrNameList[i]) == tostring(entryList[j])) then 
          local resultFiltered;
          if( G_Filter ~= nil ) then
            resultFiltered = G_Filter( ldrValueList[i], G_FunctionArgs );
    	  else
      	    resultFiltered = ldrValueList[i];
    	  end
          -- local newString = ldrNameList[i]..":"..resultFiltered; 
          -- list.append( resultList, newString );
          resultMap[ldrNameList[i]] = resultFiltered;
        end
    end 
    end -- for each remaining entry
    -- Nothing to be stored back in the LDR ctrl map 
  end
  
  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and info("!!!![DEBUG]:<%s:%s>:Result List after Search OP %s!!!!!!!!!!",
       MOD, meth, tostring( resultMap ) );
       
  -- Nothing else to be done for search, no toprec/sub-rec updates etc 
  return 0;  
end 

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result. 
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- Parms:
-- (*) objList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanListAll(topRec, ldtBinName, resultMap )

  local meth = "simpleScanListAll()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)", MOD, meth, ldtBinName);

  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  if nameList ~= nil then
    for i = 1, list.size( nameList ), 1 do
      if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
        retValue = valueList[i]; 
        if G_UnTransform ~= nil then
          retValue = G_UnTransform( valueList[i] );
        end

        local resultFiltered;
        if( G_Filter ~= nil ) then
          resultFiltered = G_Filter( retValue, G_FunctionArgs );
        else
          resultFiltered = retValue;
        end
        -- local newString = nameList[i]..":"..resultFiltered; 
        -- list.append( resultList, newString );
        resultMap[nameList[i]] = resultFiltered;
        listCount = listCount + 1; 
      end -- end if not null and not empty
    end -- end for each item in the list
  end -- end of topRec null check 

  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)
  return 0; 
end -- simpleScanListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result. 
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- Parms:
-- (*) objList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function
simpleDumpListAll(topRec, resultMap, ldtCtrl, ldtBinName )

  local meth = "simpleDumpListAll()";
  GP=E and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)
  warn("[ERROR]<%s:%s> This Method NOT READY", MOD, meth );

-- LEAVE THIS COMMENTED OUT UNTIL WE CONVERT TO RESULT MAP
--
--  local propMap = ldtCtrl[1]; 
--  local ldtMap = ldtCtrl[2]; 
--  local listCount = 0;
--  local transform = nil;
--  local unTransform = nil;
--  local retValue = nil;
--
--  -- Check once for the transform/untransform functions -- so we don't need
--  -- to do it inside the loop.
--  if ldtMap[M_Transform] ~= nil then
--    transform = functionTable[ldtMap[M_Transform]];
--  end
--
--  if ldtMap[M_UnTransform] ~= nil then
--    unTransform = functionTable[ldtMap[M_UnTransform]];
--  end
--   
--    GP=F and trace(" Parsing through :%s ", tostring(ldtBinName))
--
--	if ldtMap[M_CompactList] ~= nil then
--		local objList = ldtMap[M_CompactList];
--        list.append( resultList, "\n" );
--		for i = 1, list.size( objList ), 1 do
--                        local indexentry = "INDEX:" .. tostring(i); 
--			list.append( resultList, indexentry );
--			if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
--				retValue = objList[i]; 
--				if unTransform ~= nil then
--					retValue = unTransform( objList[i] );
--				end
--
--        			local resultFiltered;
--
--				if filter ~= nil and fargs ~= nil then
--        				resultFiltered = functionTable[func]( retValue, fargs );
--			    	else
--      					resultFiltered = retValue;
--    				end
--
--			        list.append( resultList, resultFiltered );
--				listCount = listCount + 1;
--                        else 
--			        list.append( resultList, "EMPTY ITEM" );
--			end -- end if not null and not empty
--			list.append( resultList, "\n" );
--		end -- end for each item in the list
--	end -- end of topRec null check 
--
--  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
--                 MOD, meth, listCount)
--
--  return 0; 
--
end -- simpleDumpListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result.
--
-- TODO :  
-- This is COMPLEX SCAN, currently an exact copy of the simpleScanListAll().
-- I need to first write an unTransformComplexCompare() which involves
-- using the compare function, to write a new complexScanListAll()  
--
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanListAll(topRec, ldtBinName, resultMap )
  local meth = "complexScanListAll()";
  GP=E and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  GP=F and trace(" Parsing through :%s ", tostring(ldtBinName))

  if nameList ~= nil then
    for i = 1, list.size( nameList ), 1 do
      if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
        retValue = valueList[i]; 
        if G_UnTransform ~= nil then
          retValue = G_UnTransform( valueList[i] );
        end
        local resultFiltered;

        if( G_Filter ~= nil ) then
          resultFiltered = G_Filter( retValue, G_FunctionArgs );
        else
          resultFiltered = retValue;
        end
    -- local newString = nameList[i]..":"..resultFiltered; 
	-- list.append( resultList, newString );
    resultMap[nameList[i]] = resultFiltered;
	listCount = listCount + 1; 
      end -- end if not null and not empty
    end -- end for each item in the list
  end -- end of topRec null check 

  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)
  return 0; 
end -- complexScanListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result.
--
-- TODO :  
-- This is COMPLEX SCAN, currently an exact copy of the simpleScanListAll().
-- I need to first write an unTransformComplexCompare() which involves
-- using the compare function, to write a new complexScanListAll()  
--
-- Parms:
-- Return:
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexDumpListAll(topRec, resultMap, ldtCtrl, ldtBinName )
  local meth = "complexDumpListAll()";
  GP=E and trace("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  warn("[ERROR]<%s:%s> This Method NOT READY", MOD, meth );
--  
--  if ldtMap[M_Transform] ~= nil then
--    transform = functionTable[ldtMap[M_Transform]];
--  end
--
--  if ldtMap[M_UnTransform] ~= nil then
--    unTransform = functionTable[ldtMap[M_UnTransform]];
--  end
--
--    GP=F and trace(" Parsing through :%s ", tostring(ldtBinName))
--	local binList = ldtMap[M_CompactList];
--	local resultValue = nil;
--    if topRec[ldtBinName] ~= nil then
--	        list.append( resultList, "\n" );
--		for i = 1, list.size( binList ), 1 do
--                        local indexentry = "INDEX:" .. tostring(i); 
--			list.append( resultList, indexentry );
--			if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
--				retValue = binList[i]; 
--				if unTransform ~= nil then
--					retValue = unTransform( binList[i] );
--				end
--        			local resultFiltered;
--
--				if filter ~= nil and fargs ~= nil then
--        				resultFiltered = functionTable[func]( retValue, fargs );
--			    	else
--      					resultFiltered = retValue;
--    				end
--
--			        list.append( resultList, resultFiltered );
--				listCount = listCount + 1; 
--                        else 
--			        list.append( resultList, "EMPTY ITEM" );
--			end -- end if not null and not empty
--			list.append( resultList, "\n" );
--  		end -- end for each item in the list
--    end -- end of topRec null check 
--
-- GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
--                 MOD, meth, listCount)
--
--  return 0; 
--
end -- function complexDumpListAll()

-- ======================================================================
-- regularSearch()
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- ======================================================================
local function regularSearch(topRec, ldtCtrl, searchName, resultMap )
  local meth = "regularSearch()";

  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local rc = 0; -- start out OK.

  local cellNumber = computeHashCell( searchName, ldtMap );
  local hashDirectory = ldtMap[M_HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  if ( cellAnchor[C_CellState] == C_STATE_EMPTY ) then
    warn("[WARNING]<%s:%s> Value not found for name(%s)",
      MOD, meth, tostring( searchName ) );
    error( ldte.ERR_NOT_FOUND );
  end

  local nameList;
  local valueList;
  if ( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- Get the cellAnchor lists
    nameList = cellAnchor[C_CellNameList];
    valueList = cellAnchor[C_CellValueList];
  elseif ( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
    -- Get the lists from the single Sub-Rec
    local digest = cellAnchor[C_CellDigest];
    local digestString = tostring(digest);
    local src = createSubrecContext();
    local subRec = openSubrec( src, topRec, digestString );
      
    if( subRec == nil ) then
      warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",  MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end

    local nameList = subRec[LDR_NLIST_BIN];
    local valueList = subRec[LDR_VLIST_BIN];

  else
    -- Get the lists from the correct Sub-Rec in the Radix Tree.
    -- Radix tree support not yet implemented
    info("[NOT FOUND]<%s:%s> name(%s) not found, Tree Not Ready", MOD, meth,
      tostring( searchName ));
    error( ldte.ERR_NOT_FOUND );
  end

  -- We've got a namelist to search.
  if( nameList == nil ) then
    warn("[ERROR]<%s:%s> empty Subrec NameList", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local resultObject = nil;
  local position = searchList( ldtCtrl, nameList, searchName );
  local resultFiltered = nil;
  if( position > 0 ) then
    -- ok -- found the name, so let's process the value.
    resultObject = validateValue( valueList[position] );
  end

  if( resultObject == nil ) then
    warn("[WARNING]<%s:%s> Value not found for name(%s)",
      MOD, meth, tostring( searchName ) );
    error( ldte.ERR_NOT_FOUND );
  end
  resultMap[searchName] = resultObject;
  
  -- NOTE: We could close all sub-recs here, but it really doesn't matter
  -- for a single search.
  -- ALSO -- resultMap is returned via parameter, so does not need to be
  -- returned here as a function result.

  GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
                   MOD, meth, tostring(resultMap));
end -- function regularSearch()

-- ==========================================================================
-- localLMapWalkThru()
-- ==========================================================================
-- Walk thru the LMAP and dump out contents.
-- ==========================================================================
local function localLMapWalkThru( resultList, topRec, ldtBinName )
  
  local meth = "localLMapWalkThru()";
  rc = 0; -- start out OK.
  GP=E and info("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  if ldtMap[M_StoreState] == SS_COMPACT then 
    -- Find the appropriate bin for the Search value
    GP=F and info(" !!!!!! Compact Mode LMAP Search !!!!!");
    -- local binList = ldtMap[M_CompactList];
    list.append( resultList,
      " =========== LMAP WALK-THRU COMPACT MODE \n ================" );
	  
    if ldtMap[M_KeyType] == KT_ATOMIC then
      rc = simpleDumpListAll(topRec, resultList, ldtCtrl, ldtBinName );
    else
      rc = complexDumpListAll(topRec, resultList, ldtCtrl, ldtBinName );
    end
	
    GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
    -- HACK : TODO : Fix this number to list conversion  
    local digestlist = ldtMap[M_HashDirectory];
    local src = createSubrecContext();
  
    -- for each digest in the digest-list, open that sub-rec, send it to our 
    -- routine, then get the list-back and keep appending and building the
    -- final resultList. 
     
    list.append( resultList,
          "\n =========== LMAP WALK-THRU REGULAR MODE \n ================" );
    for i = 1, list.size( digestlist ), 1 do
      if digestlist[i] ~= 0 then 
        local stringDigest = tostring( digestlist[i] );
        local digestentry = "DIGEST:" .. stringDigest; 
        list.append( resultList, digestentry );
        local IndexLdrChunk = openSubrec( src, topRec, stringDigest );
        GP=F and info("[DEBUG]: <%s:%s> Calling ldrSearchList: List(%s)",
			           MOD, meth, tostring( entryList ));
			  
        -- temporary list having result per digest-entry LDR 
        local ldrlist = list(); 
        local entryList  = list(); 
        -- The magical function that is going to fix our deletion :)
        rc = ldrSearchList(topRec,ldtBinName,ldrlist,IndexLdrChunk,0,entryList);
        if( rc == nil or rc == 0 ) then
          GP=F and info("AllSearch returned SUCCESS %s", tostring(ldrlist));
          list.append( resultList, "LIST-ENTRIES:" );
          for j = 1, list.size(ldrlist), 1 do 
            -- no need to filter here, results are already filtered in-routine
            list.append( resultList, ldrlist[j] );
          end -- for
        end -- end of if-rc check 
        rc = closeSubrec( src, stringDigest )
      else -- if digest-list is empty
        list.append( resultList, "EMPTY ITEM")
      end -- end of digest-list if check  
      list.append( resultList, "\n" );
    end -- end of digest-list for loop 
    list.append( resultList,
      "\n =========== END :  LMAP WALK-THRU REGULAR MODE \n ================" );
    -- Close ALL of the sub-recs that might have been opened
    rc = closeAllSubrecs( src );
  end -- end of else 

  return resultList;
end -- end of localLMapWalkThru

-- ======================================================================
-- convertCompactToSubRec( topRec, ldtCtrl, newName, newValue )
-- ======================================================================
-- Convert the current "Compact List" to a regular Sub-Record Hash List.
--
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
--
-- Note that we have to keep "store threshold" LOWER than the maximum number
-- of sub-records that we can have Open and Dirty at the same time.
-- 
-- We perform the following operations:
-- a. Copy the existing list into a temp-list
-- b. Add lmap related control-fields to ldtMap 
-- c. Build the sub-rec structure needed to add a list of digests
-- (fixed-size warm-list).  Note that an ESR will be created when the
-- first Sub-Record is created.
-- d. Insert records and shove into sub-recs appropriately
-- 
-- Parms:
-- (*) topRec
-- (*) ldtCtrl
-- (*) newName
-- (*) newValue
-- ======================================================================
local function convertCompactToSubRec( src, topRec, ldtCtrl, newName, newValue )
  local meth = "convertCompactToSubRec()";
  GP=E and info("[ENTER]:<%s:%s> NewName(%s) NewVal(%s)", 
     MOD, meth, tostring(newName), tostring(newValue));

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  -- If we are calling rehashSet, we probably have only one list which we
  -- can access directly with name as all LMAP bins are yser-defined names. 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local ldtBinName =  propMap[PM_BinName];

  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 

  if nameList == nil or valueList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(ldtBinName));
    error( ldte.ERR_INSERT );
  end
  
  -- Copy existing elements into temp list
  info("[DEBUG]<%s:%s> About to copy lists: Name(%s) Value(%s)", MOD, meth,
    tostring( nameList ), tostring( valueList ));
  local listNameCopy = list.take(nameList, list.size( nameList ));
  local listValueCopy = list.take(valueList, list.size( valueList ));

  info("[DEBUG]<%s:%s> Got lists: Name(%s) Value(%s)", MOD, meth,
    tostring( listNameCopy ), tostring( listValueCopy ));


  -- Create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  initializeLMapRegular( topRec, ldtCtrl )
  
  -- We could be super clever here and "batch" the inserts according to
  -- hash cell group, but then we'd have to keep this code and the regular
  -- insert code in sync -- and since the insert code will be evolving to
  -- be more clever for hash cell overflows (using a Radix tree to manage
  -- the overflow for a single hash cell) and for Hash Directory Growth
  -- (switching to linear hash growth), we'll just do the SIMPLE thing and
  -- call insert for each element and let INSERT figure out what to do.
  
  -- take-in the new element whose insertion request has triggered the rehash. 
  list.append(listNameCopy, newName);
  list.append(listValueCopy, newValue);
  
  -- Before calling code to rehash and create-sub-recs, reset COMPACT mode
  -- settings: 
  ldtMap[M_CompactNameList] = nil; 
  ldtMap[M_CompactValueList] = nil; 

  -- Notice that counts are updated ABOVE this function and "regularInsert()",
  -- so we don't have to adjust them here.

  -- Iterate thru our name/value list and perform an insert for each one.
  listSize = list.size(listNameCopy);
  for i = 1, listSize, 1 do
    regularInsert( src, topRec, ldtCtrl, listNameCopy[i], listValueCopy[i] );
  end
 
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth );
end -- convertCompactToSubRec()

-- ======================================================================
-- localPut()
-- ======================================================================
-- Insert a new element into the map.  The checking has already been done.
-- Also, the caller will do all of the control map writing/saving.
-- Parms
-- (*) src: The Sub-Record Context (tracks open sub-rec pages)
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) newName: Name to be inserted into the Large Map
-- (*) newValue: Value to be inserted into the Large Map
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- ======================================================================
-- ======================================================================
local function localPut( src, topRec, ldtCtrl, newName, newValue )
  local meth = "localPut()";
  GP=E and info("[ENTER]<%s:%s> newName(%s) newValue(%s)",
     MOD, meth, tostring(newName), tostring(newValue) );
                 
  GP=F and info("[DEBUG]<%s:%s> SRC(%s)", MOD, meth, tostring(src));

  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single list into the real sub-record organization.
  -- After the rehash (conversion), we'll drop into the regular LMAP insert.
  local totalCount = ldtMap[M_TotalCount];

  if ( ldtMap[M_StoreState] == SS_COMPACT ) then
    -- Either we insert into the COMPACT list, or we rehash
    if ( (totalCount + 1) >= ldtMap[M_Threshold] ) then
      convertCompactToSubRec( src, topRec, ldtCtrl, newName, newValue );
    else 
      compactInsert( ldtCtrl, newName, newValue );
    end
  else
    regularInsert( src, topRec, ldtCtrl, newName, newValue); 
  end

  --  NOTE: We do NOT update counts here -- our caller(s) will take
  --  care of that.
  
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    rc = 0;
  else
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end 
   
  GP=E and info("[EXIT]<%s:%s> : Done. RC(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- function localPut()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Map (LMAP) Library Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- (*) lmap.put( topRec, ldtBinName, newName, newValue, userModule) 
-- (*) lmap.put_all( topRec, ldtBinName, nameValueMap, userModule)
-- (*) lmap.get( topRec, ldtBinName, searchName, userMod, filter, fargs )
-- (*) lmap.scan( topRec, ldtBinName, userModule, filter, fargs )
-- (*) lmap.remove( topRec, ldtBinName, searchName )
-- (*) lmap.destroy( topRec, ldtBinName )
-- (*) lmap.size( topRec, ldtBinName )
-- (*) lmap.config( topRec, ldtBinName )
-- (*) lmap.set_capacity( topRec, ldtBinName, new_capacity)
-- (*) lmap.get_capacity( topRec, ldtBinName )
-- ======================================================================
-- The following functions are deprecated:
-- (*) create( topRec, ldtBinName, createSpec )
--
-- The following functions are for development use:
-- (*) lmap.dump()
-- (*) lmap.debug()
-- ======================================================================
-- We define a table of functions that are visible to both INTERNAL UDF
-- calls and to the EXTERNAL LDT functions.  We define this table, "lmap",
-- which contains the functions that will be visible to the module.
local lmap = {}
-- ======================================================================

-- ======================================================================
-- lmap.create() -- Setup a new LDT Bin in the record.
-- ======================================================================
-- Create/Initialize a Map structure in a bin, using a single LMAP
-- bin, using User's name, but Aerospike TYPE (AS_LMAP)
--
-- The LMAP starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record -- in the first lmap bin. 
-- Once the first lmap list goes over its item-count limit, we switch to 
-- standard mode and the entries get collated into a single LDR. We then
-- generate a digest for this LDR, hash this digest over N bins of a digest
-- list. 
-- Please refer to lmap_design.lua for details. 
-- 
-- Parameters: 
-- (1) topRec: the user-level record holding the LMAP Bin
-- (2) ldtBinName: The name of the LMAP Bin
-- (3) createSpec: The userModule containing the "adjust_settings()" function
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.create( topRec, ldtBinName, createSpec )
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP CREATE ] <<<<<<<<<< \n");
  local meth = "lmap.create()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring(createSpec) );
                 
  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if the Bin Name is bad.
  validateBinName( ldtBinName );

  if createSpec == nil then
    GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s) NULL createSpec",
      MOD, meth, tostring(ldtBinName));
  else
    GP=E and info("[ENTER2]: <%s:%s> ldtBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( createSpec ));
  end

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

  -- Set up a new LDT Bin
  local ldtCtrl = setupLdtBin( topRec, ldtBinName, createSpec );

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );

  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- end lmap.create()

-- ======================================================================
-- || lmap.put() -- Insert a Name/Value pair into the LMAP
-- ======================================================================
-- Put a value into the MAP.
-- Take the value, perform a hash and a modulo function to determine which
-- hash cell is used, then add to the list for that cell.
--
-- We will cache all data in the Cell ZERO until we reach a certain number N
-- (e.g. 100), and then at N+1 we will create all of the remaining cells in
-- the hash directory and redistribute the numbers, then insert the next
-- (e.g. 101th) value.  That way we save the initial storage cost of small,
-- inactive or dead users.
-- ==> The CtrlMap will show which state we are in:
-- (*) StoreState=SS_COMPACT: We are in SINGLE LIST state (no hash, cell 0)
-- (*) StoreState=SS_REGULAR: We hash, mod N, then insert into THAT cell.
--
-- Please refer to doc_lmap.md for further notes. 
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) newName: Name to be inserted into the Large Map
-- (*) newValue: Value to be inserted into the Large Map
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- ======================================================================
function lmap.put( topRec, ldtBinName, newName, newValue, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP PUT ] <<<<<<<<<< \n");
  local meth = "lmap.put()";
  GP=E and trace("[ENTRY]<%s:%s> Bin(%s) name(%s) value(%s) module(%s)",
    MOD, meth, tostring(ldtBinName), tostring(newName),tostring(newValue),
    tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO] <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read/Write Functions (KeyFunction, Transform, Untransform)
  setReadFunctions( ldtMap, nil, nil, nil );
  setWriteFunctions( ldtMap );
  
  -- Needed only when we're in sub-rec mode, but that will be most of the time.
  local src = createSubrecContext();

  rc = localPut( src, topRec, ldtCtrl, newName, newValue );

  -- Update the counts.  If there were any errors, the code would have
  -- jumped out of the Lua code entirely.  So, if we're here, the insert
  -- was successful.
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
  ldtMap[M_TotalCount] = totalCount + 1; -- Total number of items goes up
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    rc = 0;
    GP=E and info("[EXIT]: <%s:%s> Success", MOD, meth );      
  else
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end 
   
  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmap.put()

-- ======================================================================
-- put_all() -- Insert multiple name/value pairs into the LMAP
-- ======================================================================
-- This now uses localPut() to write, so now we can return a VECTOR of
-- errors in the event of any one insert going badly.
-- In fact, if the ERROR is not severe (e.g. a duplicate value error),
-- then we should keep on going.
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) nameValMap: The Map containing all of the new Name/Value pairs
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- ======================================================================
function lmap.put_all( topRec, ldtBinName, nameValMap, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP PUT ALL] <<<<<<<<<< \n");

  local meth = "lmap.put_all()";
   
  GP=E and trace("[ENTRY]<%s:%s> Bin(%s) name(%s) value(%s) module(%s)",
    MOD, meth, tostring(ldtBinName), tostring(newName),tostring(newValue),
    tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO] <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read/Write Functions (KeyFunction, Transform, Untransform)
  setReadFunctions( ldtMap, nil, nil, nil );
  setWriteFunctions( ldtMap );

  -- Needed only when we're in sub-rec mode, but that will be most of the time.
  local src = createSubrecContext();

  local newCount = 0;
  for name, value in map.pairs( nameValMap ) do
    GP=F and info("[DEBUG]<%s:%s> Processing Arg: Name(%s) Val(%s) TYPE : %s",
        MOD, meth, tostring( name ), tostring( value ), type(value));
    rc = localPut( src, topRec, ldtCtrl, name, value );
    -- We need to drop out of here if there's an error, but we have to do it
    -- carefully because all previous PUTS must have succeeded.  So, we
    -- should really return a VECTOR of return status!!!!
    -- TODO: Return a VECTOR of error status and jump out with that vector
    -- on error!!
    if( rc == 0 ) then
      newCount = newCount + 1;
      GP=F and info("[DEBUG]<%s:%s> lmap insertion for %s %s RC(%d)",
        MOD, meth, tostring(name), tostring(value), rc );
    else
      GP=F and info("[ERROR]<%s:%s> lmap insertion for %s %s RC(%d)",
        MOD, meth, tostring(name), tostring(value), rc );
    end
  end -- for each new value in the map

  -- Update the counts.  If there were any errors, the code would have
  -- jumped out of the Lua code entirely.  So, if we're here, the insert
  -- was successful.
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount + newCount; -- number of valid items goes up
  ldtMap[M_TotalCount] = totalCount + newCount; -- Total number of items goes up
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    rc = 0;
    GP=E and info("[EXIT]: <%s:%s> Success", MOD, meth );      
  else
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end 
   
  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmap.put_all()

-- ======================================================================
-- lmap.get() -- Return a map containing the searched-for name/value pair.
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) searchname:
-- (*) userModule:
-- (*) filter:
-- (*) fargs:
-- ======================================================================
function
lmap.get(topRec, ldtBinName, searchName, userModule, filter, fargs)
  GP=B and info("\n\n >>>>>>>>> API[ LMAP GET] <<<<<<<<<< \n");
  local meth = "lmap.get()";
  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local resultMap = map(); -- add results to this list.
  local rc = 0; -- start out OK.
  
  -- Set up the Read Functions (UnTransform, Filter)
  setReadFunctions( ldtMap, userModule, filter, fargs );

  -- Process these two options differently.  Either we're in COMPACT MODE,
  -- which means have two simple lists connected to the LDT BIN, or we're
  -- in REGULAR_MODE, which means we're going to open up a SubRecord and
  -- read the lists in there.
  if ldtMap[M_StoreState] == SS_COMPACT then 
    local nameList = ldtMap[M_CompactNameList];
    local position = searchList( ldtCtrl, nameList, searchName );
    local resultObject = nil;
    if( position > 0 ) then
      local valueList = ldtMap[M_CompactValueList];
      resultObject = validateValue( valueList[position] );
    end
    if( resultObject == nil ) then
      info("[NOT FOUND]<%s:%s> name(%s) not found",
        MOD, meth, tostring(searchName));
      error( ldte.ERR_NOT_FOUND );
    end
    resultMap[nameList[position]] = resultObject;
  else
    -- Search the SubRecord.
    regularSearch( topRec, ldtCtrl, searchName, resultMap );
  end

  GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
     MOD, meth, tostring(resultMap));

  return resultMap;
end -- function lmap.get()

-- ========================================================================
-- lmap.scan() -- Return a map containing ALL name/value pairs.
-- ========================================================================
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) userModule:
-- (*) filter:
-- (*) fargs:
-- ========================================================================
-- THIS CODE MUST BE CHECKED AND VERIFIED !!!
-- TODO: FIX THIS CODE
-- ========================================================================
function lmap.scan(topRec, ldtBinName, userModule, filter, fargs)
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP SCAN ] <<<<<<<<<< \n");

  local meth = "lmap.scan()";
  rc = 0; -- start out OK.
  GP=E and trace("[ENTER]: <%s:%s> Bin-Name: %s Search for Value(%s)",
                 MOD, meth, tostring(ldtBinName), tostring( searchValue ));
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local resultMap = map();

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read Functions (UnTransform, Filter)
  setReadFunctions( ldtMap, userModule, filter, fargs );

  if ldtMap[M_StoreState] == SS_COMPACT then 
    -- Find the appropriate bin for the Search value
    GP=F and trace(" !!!!!! Compact Mode LMAP Search Key-Type: %s !!!!!",
      tostring(ldtMap[M_KeyType]));
    -- local binList = ldtMap[M_CompactList];
	  
    if ldtMap[M_KeyType] == KT_ATOMIC then
      rc = simpleScanListAll(topRec, ldtBinName, resultMap );
    else
      rc = complexScanListAll(topRec, ldtBinName, resultMap );
    end
	
    GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
    -- HACK : TODO : Fix this number to list conversion  
    local digestlist = ldtMap[M_HashDirectory];
    local src = createSubrecContext();
	
    -- for each digest in the digest-list, open that sub-rec, send it to our 
    -- routine, then get the list-back and keep appending and building the
    -- final resultMap. 
	  
    for i = 1, list.size( digestlist ), 1 do
      if digestlist[i] ~= 0 then 
        local stringDigest = tostring( digestlist[i] );
        local IndexLdrChunk = openSubrec( src, topRec, stringDigest );
        GP=F and info("[DEBUG]: <%s:%s> Calling ldrSearchList", MOD, meth);
			  
        -- temporary list having result per digest-entry LDR 
        local ldrlist = list(); 
        local entryList  = list(); 
        -- The magical function that is going to fix our deletion :)
        rc = ldrSearchList(topRec, ldtBinName, resultMap, IndexLdrChunk,
          0, entryList );
        if( rc == nil or rc == 0 ) then
       	  GP=F and info("AllSearch returned SUCCESS %s", tostring(ldrlist));
          break;
         end -- end of if-else check 
         rc = closeSubrec( src, stringDigest )
       end -- end of digest-list if check  
     end -- end of digest-list for loop 
     -- Close ALL of the sub-recs that might have been opened
     rc = closeAllSubrecs( src );
  end -- end of else 
  	  
  return resultMap;
end -- function lmap.scan()

-- ======================================================================
-- lmap.remove() -- Remove the name/value pair matching <searchName>
-- ======================================================================
-- Delete a value from the MAP.
-- Find the value (bin and structure), then remove it.
--
-- If StoreState is compact, then we know to look in the compact list.
-- Otherwise, we'll search the right list for this hash value.
--
-- Please refer to lmap_design.lua for further notes. 
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- ======================================================================
function
lmap.remove( topRec, ldtBinName, searchName, userModule, filter, fargs )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");

  local meth = "lmap.remove()";
   
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) name(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchName));

  GP=E and trace("[DEBUG]<%s:%s> userModule(%s) filter(%s) fargs(%s)",
    MOD, meth, tostring(userModule), tostring(filter),tostring(fargs));

  local resultMap = map();  -- add results to this list.

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read Functions (filter, Untransform)
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  -- For the compact list, it's a simple list delete (if we find it).
  -- For the subRec list, it's a more complicated search and delete.
  local resultMap = map();
  if ldtMap[M_StoreState] == SS_COMPACT then
    rc = compactDelete( ldtCtrl, searchName, resultMap );
  else
    -- It's "regular".  Find the right LDR (subRec) and search it.
    rc = subRecDelete( topRec, ldtCtrl, searchName, resultMap );
  end


  -- Update the counts.  If there were any errors, the code would have
  -- jumped out of the Lua code entirely.  So, if we're here, the delete
  -- was successful.
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount - 1; -- number of valid items goes down
  ldtMap[M_TotalCount] = totalCount - 1; -- Total number of items goes up
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, update the record
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    rc = 0;
    GP=E and info("[EXIT]: <%s:%s> Success", MOD, meth );      
  else
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end 
   
  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmap.remove()

-- ========================================================================
-- lmap.destroy() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the Ldt Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NOTE: This could eventually be moved to COMMON, and be "ldt.destroy()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
function lmap.destroy( topRec, ldtBinName )
  local meth = "lmap.destroy()";

  GP=B and info("\n\n  >>>>>>>> API[ LMAP DESTROY ] <<<<<<<<<<<<<<<<<< \n");

  GP=E and info("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and Ldt control map from the Ldt bin list.

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  GD=DEBUG and ldtDebugDump( ldtCtrl );
  
  GP=F and info("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), ldtSummaryString( ldtCtrl ));

  -- If Regular mode (not compact), then we have to remove the ESR.  Once
  -- the ESR is removed, all of the sub-records will automatically be
  -- cleaned up by the NameSpace Supervisor (NSUP).
  if ldtMap[M_StoreState] ~= SS_COMPACT then 
  	-- Get the ESR and delete it.
    local esrDigest = propMap[PM_EsrDigest];
    local esrDigestString = tostring(esrDigest);
    local esrRec = aerospike:open_subrec( topRec, esrDigestString );
    GP=F and info("[STATUS]<%s:%s> Calling Aerospike REMOVE", MOD, meth );
    if( esrRec ~= nil ) then
      rc = aerospike:remove_subrec( esrRec );
    end
    if( rc == nil or rc == 0 ) then
      GP=F and info("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
    else
      warn("[ERROR]<%s:%s> Delete Error RC(%d) Bin(%s)",
        MOD, meth, rc, ldtBinName);
      error( ldte.ERR_SUBREC_DELETE );
    end
  end 

  -- Mark the enitre control-info structure nil 
  topRec[ldtBinName] = nil;

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.

  local recPropMap = topRec[REC_LDT_CTRL_BIN];

  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Bin invalid, Contents %s",
      MOD, meth, tostring(recPropMap) );
    error( ldte.ERR_BIN_DAMAGED );
  end

  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- Remove this bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  end
  
  rc = aerospike:update( topRec );
  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%s)", MOD, meth, tostring(rc));

  return rc;
end -- function lmap.destroy()

-- ========================================================================
-- lmap.size() -- return the number of elements (item count) in the LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = size is greater or equal to 0.
--   res = -1: Some sort of error
-- ========================================================================
function lmap.size( topRec, ldtBinName )
  local meth = "size()";
  GP=B and info("\n\n >>>>>>>>> API[ LMAP SIZE ] <<<<<<<<<< \n");

  GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local itemCount = propMap[PM_ItemCount];

  GD=DEBUG and ldtDebugDump( ldtCtrl );

  GP=E and trace("[EXIT]: <%s:%s> : SIZE(%d)", MOD, meth, itemCount );
  return itemCount;
end -- function lmap.size()

-- ========================================================================
-- lmap.config() -- return the config settings
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = Map of config settings
--   res = -1: Some sort of error
-- ========================================================================
function lmap.config( topRec, ldtBinName )
  local meth = "lmap.config()";
  GP=B and info("\n\n >>>>>>>>> API[ LMAP CONFIG ] <<<<<<<<<< \n");

  GP=E and info("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local config = ldtSummary(ldtCtrl); 

  GP=E and info("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config) );
  return config;
end -- function lmap.config();

-- ========================================================================
-- lmap.get_capacity() -- return the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.get_capacity( topRec, ldtBinName )
  local meth = "lmap.get_capacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and Ldt control map from the Ldt bin list.
  local ldtMap = ldtCtrl[2];
  local capacity = ldtMap[M_StoreLimit];
  if( capacity == nil ) then
    capacity = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function lmap.get_capacity()

-- ========================================================================
-- set_capacity() -- set the current capacity setting for this LDT
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.set_capacity( topRec, ldtBinName, capacity )
  local meth = "lmap.set_capacity()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and Ldt control map from the Ldt bin list.
  local ldtMap = ldtCtrl[2];
  if( capacity ~= nil and type(capacity) == "number" and capacity >= 0 ) then
    ldtMap[M_StoreLimit] = capacity;
  else
    warn("[ERROR]<%s:%s> Bad Capacity Value(%s)",MOD,meth,tostring(capacity));
    error( ldte.ERR_INTERNAL );
  end

  GP=E and trace("[EXIT]: <%s:%s> : new size(%d)", MOD, meth, capacity );

  return 0;
end -- function lmap.set_capacity()

-- ========================================================================
-- ========================================================================
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- (*) debug()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
--
-- Dump the full contents of the Large Map, with Separate Hash Groups
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function lmap.dump( topRec, ldtBinName )
  GP=F and info("\n\n  >>>>>>>>>>>> API[ LMAP DUMP ] <<<<<<<<<<<<<<<< \n");
  local meth = "dump()";
  GP=E and info("[ENTER]<%s:%s> BIN(%s)", MOD, meth, tostring(ldtBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  resultList = list();
  list.append( resultList, "EMPTY LIST");

  warn("[ERROR]<%s:%s> not yet implemented", MOD, meth );
  -- localLMapWalkThru(resultList,topRec,ldtBinName,nil,nil);

  for i = 1, list.size( resultList ), 1 do
     info(tostring(resultList[i]));
  end 

  -- Another key difference between dump and scan : 
  -- dump prints things in the logs and returns a 0
  -- scan returns the list to the client/caller 

  local ret = " \n LDT bin contents dumped to server-logs \n"; 
  return ret; 
end -- function lmap.dump();

-- ========================================================================
-- lmap.debug() -- Turn the debug setting on (1) or off (0)
-- ========================================================================
-- Turning the debug setting "ON" pushes LOTS of output to the console.
-- It would be nice if we could figure out how to make this setting change
-- PERSISTENT. Until we do that, this will be a no-op.
-- Parms:
-- (1) topRec: the user-level record holding the Ldt Bin
-- (2) setting: 0 turns it off, anything else turns it on.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function lmap.debug( topRec, setting )
  local meth = "lmap.debug()";
  local rc = 0;

  GP=E and trace("[ENTER]: <%s:%s> setting(%s)", MOD, meth, tostring(setting));
  if( setting ~= nil and type(setting) == "number" ) then
    if( setting == 1 ) then
      info("[DEBUG SET]<%s:%s> Turn Debug ON", MOD, meth );
      F = true;
      B = true;
      E = true;
      DEBUG = true;
    elseif( setting == 0 ) then
      info("[DEBUG SET]<%s:%s> Turn Debug OFF", MOD, meth );
      F = false;
      B = false;
      E = false;
      DEBUG = false;
    else
      info("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
      rc = -1;
    end
  else
    info("[DEBUG SET]<%s:%s> Unknown Setting(%s)",MOD,meth,tostring(setting));
    rc = -1;
  end
  return rc;
end -- function lmap.debug()

-- ======================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return lmap;
-- ========================================================================
--   _     ___  ___  ___  ______ 
--  | |    |  \/  | / _ \ | ___ \
--  | |    | .  . |/ /_\ \| |_/ /
--  | |    | |\/| ||  _  ||  __/ 
--  | |____| |  | || | | || |    
--  \_____/\_|  |_/\_| |_/\_|    (LIB)
--                               
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
