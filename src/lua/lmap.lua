-- Large Map (LMAP) Operations
-- Track the data and iteration of the last update.
local MOD="lmap_2013_09_19.c";

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.1;

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
-- Cell, and then search that subrecord for the name.
-- Each Subrecord contains two data lists, one for names and one for values.

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true; -- Leave this ALWAYS true (but value seems not to matter)
local F=true; -- Set F (flag) to true to turn ON global print
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=true; -- Set B (Banners) to true to turn ON Banner Print

-- ======================================================================
-- !! Please refer to lmap_design.lua for architecture and design notes!! 
-- ======================================================================
--
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LMAP Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LMAP module:
--
-- (*) Status = put( topRec, ldtBinName, newName, newValue, userModule) 
-- (*) Status = put_all( topRec, ldtBinName, nameValueMap, userModule)
-- (*) Map    = get( topRec, ldtBinName, searchName )
-- (*) Map    = scan( topRec, ldtBinName )
-- (*) Map    = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchName )
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Number = get_capacity( topRec, ldtBinName )
-- ======================================================================
--
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

-- We have a set of packaged settings for each LDT
local lmapPackage = require('settings_lmap');

-- Get addressability to the Function Table: Used for compress and filter
-- set up our "outside" links
local  CRC32 = require('CRC32');

-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
-- This flavor of LDT
local LDT_TYPE_LMAP = "LMAP";

-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- In this early version of MAP, we distribute values among lists that we
-- keep in the top record.  This is the default modulo value for that list
-- distribution.   Later we'll switch to a more robust B+ Tree version.
local DEFAULT_DISTRIB = 31;
-- Switch from a single list to distributed lists after this amount
local DEFAULT_THRESHOLD = 100;

local MAGIC="MAGIC";     -- the magic value for Testing LSO integrity

-- Common LDT functions that are used by ALL of the LDTs.
-- local LDTC = require('ldt_common');
local ldte=require('ldt_errors');

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY  ='B'; -- Using a Transform function to compact values
local SM_LIST    ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
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

-- We maintain a pool, or "context", of subrecords that are open.  That allows
-- us to look up subrecs and get the open reference, rather than bothering
-- the lower level infrastructure.  There's also a limit to the number
-- of open subrecs.
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
local PM_SubRecCount           = 'S'; -- (Top): # of subrecs in the LDT
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
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lset & lmap 
local M_StoreMode              = 'M';
local M_StoreLimit             = 'L'; -- Used for Eviction (eventually)
local M_UserModule             = 'P'; -- User's Lua file for overrides
local M_Transform              = 't'; -- Transform Lua to Byte format
local M_UnTransform            = 'u'; -- UnTransform from Byte to Lua format
local M_LdrEntryCountMax       = 'e'; -- Max # of items in an LDR
local M_LdrByteEntrySize       = 's';
local M_LdrByteCountMax        = 'b';
local M_StoreState             = 'S'; 
local M_BinaryStoreSize        = 'B'; 
local M_KeyType                = 'K'; 
local M_TotalCount             = 'N'; 
local M_Modulo                 = 'O';
local M_ThreshHold             = 'H';
local M_KeyFunction            = 'K'; -- User Supplied Key Extract Function
local M_CompactNameList        = 'n';--Simple Compact List -- before "dir mode"
local M_CompactValueList       = 'v';--Simple Compact List -- before "dir mode"

-- Fields specific to lmap in the standard mode only. In standard mode lmap 
-- does not resemble lset, it looks like a fixed-size warm-list from lstack
-- with a digest list pointing to LDR's. 

local M_DigestList             = 'W';-- The Directory of Hash Entries
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
-- Maintain the LSO letter Mapping here, so that we never have a name
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
-- H:M_Threshold              h:                        7:
-- I:                         i:                        8:
-- J:                         j:                        9:
-- K:M_KeyFunction            k:                  
-- L:M_StoreLimit             l:M_ListDigestCount
-- M:M_StoreMode              m:
-- N:M_TotalCount             n:M_CompactNameList
-- O:                         o:
-- P:M_UserModule             p:
-- Q:                         q:
-- R:M_ColdDataRecCount       r:
-- S:M_StoreLimit             s:M_LdrByteEntrySize
-- T:                         t:M_Transform
-- U:                         u:M_UnTransform
-- V:                         v:M_CompactValueList
-- W:M_DigestList             w:                     
-- X:                         x:                    
-- Y:                         y:
-- Z:                         z:
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
-- Note the 14 character limit on Aerospike Bin Names.
--                         123456789ABCDE
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_NLIST_BIN     = "LdrNListBin";  
local LDR_VLIST_BIN     = "LdrVListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- All LDT subrecords have a properties bin that holds a map that defines
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
  GP=E and trace("[ENTER]<%s:%s> Process Filter(%s)",
    MOD, meth, tostring(filter));

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


-- -----------------------------------------------------------------------
-- ------------------------------------------------------------------------
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
  GP=E and info("[ENTER]<%s:%s>", MOD, meth );

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

  GP=E and info("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return srcList;
end -- createSubrecContext()

-- ======================================================================
-- Given an already opened subrec (probably one that was recently created),
-- add it to the subrec context.
-- ======================================================================
local function addSubrecToContext( srcList, subrec )
  local meth = "addSubrecContext()";
  GP=E and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring( srcList));

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

  GP=E and info("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- openSubrec()
-- ======================================================================
local function openSubrec( srcList, topRec, digestString )
  local meth = "openSubrec()";
  GP=E and info("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
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
    GP=F and info("[OPEN SUBREC]<%s:%s>SRC.ItemCount(%d) TR(%s) DigStr(%s)",
      MOD, meth, recMap.ItemCount, tostring(topRec), digestString );
    subrec = aerospike:open_subrec( topRec, digestString );
    GP=F and info("[OPEN SUBREC RESULTS]<%s:%s>(%s)", 
      MOD,meth,tostring(subrec));
    if( subrec == nil ) then
      warn("[ERROR]<%s:%s> Subrec Open Failure: Digest(%s)", MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
  else
    GP=F and info("[FOUND REC]<%s:%s>Rec(%s)", MOD, meth, tostring(subrec));
  end

  GP=E and info("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
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
  GP=E and info("[ENTER]<%s:%s> DigestStr(%s) SRC(%s)",
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
    return rc;
    -- error( ldte.ERR_INTERNAL );
  end

  GP=F trace("[STATUS]<%s:%s> Closing Rec: Digest(%s)", MOD, meth, digestString);

  if( dirtyStatus == true ) then
    warn("[WARNING]<%s:%s> Can't close Dirty Record: Digest(%s)",
      MOD, meth, digestString);
  else
    rc = aerospike:close_subrec( subrec );
    GP=F and info("[STATUS]<%s:%s>Closed Rec: Digest(%s) rc(%s)", MOD, meth,
      digestString, tostring( rc ));
  end

  GP=E and info("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
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
  --GP=E and info("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
 --   MOD, meth, tostring(topRec), digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local rc = 0;

  if( digest == nil or digest == 0 ) then
    digest = record.digest( subrec );
  end
  local digestString = tostring( digest );

  rc = aerospike:update_subrec( subrec );
  dirtyMap[digestString] = true;

  GP=E and info("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- updateSubrec()

-- ======================================================================
-- markSubrecDirty()
-- ======================================================================
local function markSubrecDirty( srcList, digestString )
  local meth = "markSubrecDirty()";
  GP=E and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  -- Pull up the dirtyMap, find the entry for this digestString and
  -- mark it dirty.  We don't even care what the existing value used to be.
  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  dirtyMap[digestString] = true;
  
  GP=E and info("[EXIT]<%s:%s> SRC(%s)", MOD, meth, tostring(srcList) );
  return 0;
end -- markSubrecDirty()

-- ======================================================================
-- closeAllSubrecs()
-- ======================================================================
local function closeAllSubrecs( srcList )
  local meth = "closeAllSubrecs()";
  GP=E and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  -- Iterate thru the SubRecContext and close all subrecords.
  local digestString;
  local rec;
  local rc = 0;
  for name, value in map.pairs( recMap ) do
    GP=F and info("[DEBUG]: <%s:%s>: Processing Pair: Name(%s) Val(%s)",
      MOD, meth, tostring( name ), tostring( value ));
    if( name == "ItemCount" ) then
      GP=F and info("[DEBUG]<%s:%s>: Processing(%d) Items", MOD, meth, value);
    else
      digestString = name;
      rec = value;
      GP=F and info("[DEBUG]<%s:%s>: Would have closed SubRec(%s) Rec(%s)",
      MOD, meth, digestString, tostring(rec) );
      -- GP=F and info("[DEBUG]<%s:%s>: Closing SubRec: Digest(%s) Rec(%s)",
      --   MOD, meth, digestString, tostring(rec) );
      -- rc = aerospike:close_subrec( rec );
      -- GP=F and info("[DEBUG]<%s:%s>: Closing Results(%d)", MOD, meth, rc );
    end
  end -- for all fields in SRC

  GP=E and info("[EXIT]: <%s:%s> : RC(%s)", MOD, meth, tostring(rc) );
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
  GP=E and info("[ENTER]<%s:%s> value(%s)",
       MOD, meth, tostring(value) );

  GP=F and info(" Ctrl-Map : %s", tostring(ldtMap));

  local keyValue;
  if ldtMap[M_KeyType] == KT_ATOMIC then
    keyValue = value;
  else
    -- Employ the user's supplied function (keyFunction) and if that's not
    -- there, look for the special case where the object has a field
    -- called 'key'.  If not, then, well ... tough.  We tried.
    local keyFunction = ldtMap[M_KeyFunction];

    -- WE ARE DEALING WITH A NAME:VALUE PAIR HERE !!!!!!!!!

    if( keyFunction ~= nil ) and functionTable[keyFunction] ~= nil then
        GP=F and info(" !!! Key Function Specified !!!!! ");
      keyValue = functionTable[keyFunction]( value );
    elseif value ~= nil then
      -- WE ARE DEALING WITH A NAME:VALUE PAIR HERE !!!!!!!!!
      -- USE THE STRING OF THE ENTIRE MAP OBJECT AS OUR KEY 
      keyValue = tostring(value); 
    else
      keyValue = -1;
    end
  end

  GP=E and info("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
  return tostring(keyValue);
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
  GP=E and info("[ENTER]: <%s:%s> transform(%s) type(%s) Value(%s)",
 MOD, meth, tostring(transformFunc), type(transformFunc), tostring(newValue));

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
local function unTransformComplexCompare(ldtMap, unTransform, dbValue, searchKey)
  local meth = "unTransformComplexCompare()";

  GP=E and info("[ENTER]: <%s:%s> unTransform(%s) dbVal(%s) key(%s)",
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
-- local function ldtSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtCtrl 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
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

  -- Properties
  -- Fields common for all LDT's
  resultMap.SUMMARY              = "LMAP Summary";
  resultMap.PropItemCount        = propMap[PM_ItemCount];
  resultMap.PropSubRecCount      = propMap[PM_SubRecCount];
  resultMap.PropVersion          = propMap[PM_Version];
  resultMap.PropLdtType          = propMap[PM_LdtType];
  resultMap.PropBinName          = propMap[PM_BinName];
  resultMap.Magic                = propMap[PM_Magic];
  resultMap.PropEsrDigest        = propMap[PM_EsrDigest];
  resultMap.RecType              = propMap[PM_RecType];
  --resultMap.LogInfo              = propMap[PM_LogInfo];
  resultMap.ParentDigest         = propMap[PM_ParentDigest];
  resultMap.SelfDigest           = propMap[PM_SelfDigest];
  
  -- General LMAP Parms:
  resultMap.StoreMode            = ldtMap[M_StoreMode];
  resultMap.Transform            = ldtMap[M_Transform];
  resultMap.UnTransform          = ldtMap[M_UnTransform];
  resultMap.UserModule           = ldtMap[M_UserModule];
  resultMap.BinaryStoreSize      = ldtMap[M_BinaryStoreSize];
  resultMap.KeyType              = ldtMap[M_KeyType];
  resultMap.TotalCount	         = ldtMap[M_TotalCount];		
  resultMap.Modulo 		         = ldtMap[M_Modulo];
  resultMap.ThreshHold		     = ldtMap[M_ThreshHold];
  
  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = ldtMap[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[M_LdrByteCountMax];

  -- Digest List Settings: List of Digests of LMAP Data Records
  -- specific to LMAP in STANDARD_MODE ONLY 
  
  resultMap.DigestList        = ldtMap[M_DigestList];
  resultMap.TopFull 	      = ldtMap[M_TopFull];
  resultMap.ListDigestCount   = ldtMap[M_ListDigestCount];
  resultMap.ListMax           = ldtMap[M_ListMax];
  resultMap.TopChunkByteCount = ldtMap[M_TopChunkByteCount];
  resultMap.TopChunkEntryCount= ldtMap[M_TopChunkEntryCount];

  return resultMap;
end -- ldtSummary()

-- ======================================================================
-- Make it easier to use lsoSummary(): Have a String version.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
    return tostring( ldtSummary( ldtCtrl ) );
end

-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- ======================================================================
local function setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";
  GP=E and info("[ENTER]<%s:%s>", MOD, meth );

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if( topRec[REC_LDT_CTRL_BIN] == nil ) then
    GP=F and info("[DEBUG]<%s:%s>Creating Record LDT Map", MOD, meth );

    -- If this record doesn't even exist yet -- then create it now.
    -- Otherwise, things break.
    if( not aerospike:exists( topRec ) ) then
      GP=F and info("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
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
    GP=F and info("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1 );
  end

  topRec[REC_LDT_CTRL_BIN] = recPropMap;    
  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );
    if( rc == nil or rc == 0 ) then
      GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 

  GP=E and info("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
  return rc;
end -- setLdtRecordType()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two main Record Types used in the LSO Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSO bin
-- (*) ldtBinName: the LSO Data Record that holds user Data
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
  
  GP=E and info("[ENTER]: <%s:%s>:: ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));
  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM_SubRecCount] = 0;
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LMAP; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = ldtBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]  = nil; -- not set yet.
  propMap[PM_SelfDigest] = nil; 
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
--  propMap[PM_CreateTime] = aerospike:get_current_time();
  warn("WARNING:: Please Fix GET CURRENT TIME");
  propMap[PM_CreateTime] = 0;
  
-- Specific LMAP Parms: Held in LMap
  ldtMap[M_StoreMode]  = SM_LIST; -- SM_LIST or SM_BINARY:
  ldtMap[M_StoreLimit]  = nil; -- No storage Limit

  -- LMAP Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[M_LdrEntryCountMax] = 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[M_LdrByteEntrySize] =  0;  -- Byte size of a fixed size Byte Entry
  ldtMap[M_LdrByteCountMax]  =   0; -- Max # of Data Chunk Bytes (binary mode)

  ldtMap[M_Transform]        = nil; -- applies only to complex lmap
  ldtMap[M_UnTransform]      = nil; -- applies only to complex lmap
  ldtMap[M_StoreState]       = SS_COMPACT; -- SM_LIST or SM_BINARY:
  ldtMap[M_HashType]         = HT_STATIC; -- Static or Dynamic
  ldtMap[M_BinaryStoreSize]  = nil; 
  ldtMap[M_KeyType]          = KT_ATOMIC; -- assume "atomic" values for now.
  ldtMap[M_TotalCount]       = 0; -- Count of both valid and deleted elements
  ldtMap[M_Modulo]           = DEFAULT_DISTRIB; -- Currently this is 31
  -- Rehash after this many have been inserted
  ldtMap[M_ThreshHold]       = DEFAULT_THRESHOLD;
  -- name-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[M_CompactNameList]  = list();
  -- value-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[M_CompactValueList] = list();
	  
  
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

  GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  GP=E and info("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtCtrl;
  
end -- initializeLdtCtrl()

-- ======================================================================
-- ======================================================================
local function initializeLMapRegular(topRec, ldtBinName)
  local meth = "initializeLMapRegular()";
  
  GP=E and info("[ENTER]: <%s:%s>:: Regular Mode ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));
  
  local ldtCtrl = topRec[ldtBinName] ; -- The main lsoMap structure

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = ldtCtrl[1];
  local ldtMap  = ldtCtrl[2];
  
  -- we are now in rehashSettoLmap(), we need to change ldtMap params  
  -- all the other params must already be set by default. 
 
  GP=F and info("[DEBUG]: <%s:%s>:: Regular-Mode ldtBinName(%s) Key-type: %s",
      MOD, meth, tostring(ldtBinName), tostring(ldtMap[M_KeyType]));

  ldtMap[M_StoreState]  = SS_REGULAR; -- SM_LIST or SM_BINARY:
  	  
  -- Digest List Settings: List of Digests of LMAP Data Records
  propMap[PM_ParentDigest]          = 'P'; -- (Subrec): Digest of TopRec
  propMap[PM_SelfDigest]            = 'D'; -- (Subrec): Digest of THIS Record
  ldtMap[M_DigestList]        = list(); -- the list of digests for LDRs
  
  -- true when the list of entries pointed to by a digest is full (for next write)
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

  -- Do we need this topRec assignment here ?
  -- TODO : Ask Toby about it 
 
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after Init(%s)",
       MOD, meth , ldtSummaryString(ldtCtrl));

  GP=E and info("[EXIT]:<%s:%s>:", MOD, meth );
  
end 

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
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- for lmap_create, mustExist is false
-- This also gets called for any other lmap-param like search, insert, delete etc 
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateRecBinAndMap( topRec, ldtBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=E and info("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
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
  -- TODO : Flag is true for peek, trim, config, size, delete etc 
  -- Those functions must be added b4 we validate this if section 
  if mustExist == true then
    -- Check Top Record Existence.

    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
     
    -- Control Bin Must Exist
    if( topRec[ldtBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LMAP BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    local ldtCtrl = topRec[ldtBinName] ; -- The main lsoMap structure

    -- Extract the property map and lso control map from the lso bin list.
    local propMap = ldtCtrl[1];
    local ldtMap  = ldtCtrl[2];

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
      local ldtCtrl = topRec[ldtBinName]; -- The main lsoMap structure
      -- Extract the property map and lso control map from the lso bin list.
      local propMap = ldtCtrl[1];
      local ldtMap  = ldtCtrl[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=E and warn("[ERROR EXIT]:<%s:%s> LMAP BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( ldtBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
    end -- if worth checking
  end -- else for must exist
  
end -- validateRecBinAndMap()

-- ======================================================================
-- adjustLdtMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LsoMap:
-- Parms:
-- (*) ldtCtrl: the main LSO Bin value (propMap, ldtMap)
-- (*) argListMap: Map of LSO Settings 
-- Return: The updated LsoList
-- ======================================================================
local function adjustLdtMap( ldtCtrl, argListMap )
  local meth = "adjustLdtMap()";
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  GP=E and trace("[ENTER]: <%s:%s>:: LsoList(%s)::\n ArgListMap(%s)",
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

  GP=E and trace("[EXIT]:<%s:%s>:LsoList after Init(%s)",
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
-- Search a list for an item.  Each object (atomic or complex) is translated
-- into a "searchKey".  That can be a hash, a tostring or any other result
-- of a "uniqueIdentifier()" function.
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
-- THIS IS NO LONGER USED (tjl)
-- This gets called after every lmap_create to set the self-digest and update 
-- TODO : Ask Toby if this can be done in another way 
-- DONE : You just need to assign the ldtCtrl back into the record, but you
--        do NOT need to create a new ldtCtrl.
-- ======================================================================
-- local function lmap_update_topdigest( topRec, binName )
--     local meth = "lmap_update_topdigest()";
--     local ldtCtrl = topRec[binName] ;
--     local propMap = ldtCtrl[1]; 
--     local ldtMap = ldtCtrl[2];
--     propMap[PM_SelfDigest]   = record.digest( topRec );
-- 
--     topRec[binName] = ldtCtrl;
--     record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time
-- 
--     rc = aerospike:update( topRec );
--     if( rc == nil or rc == 0 ) then
--       GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
--     else
--       warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
--       error( ldte.ERR_SUBREC_UPDATE );
--     end 
--     GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
--     return rc;
-- end -- lmap_update_topdigest()

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

  ldtMap[M_CompactNameList] = list();
  ldtMap[M_CompactValueList] = list(); 

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
  GP=E and info("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
  return result
end -- numberHash

-- ======================================================================
-- computeSetBin()
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single bin.
-- Second -- use the right hash function (depending on the type).
-- And, know if it's an atomic type or complex type.
-- ======================================================================
local function computeSetBin( newValue, ldtMap )
  local meth = "computeSetBin()";
  GP=E and info("[ENTER]: <%s:%s> val(%s) type = %s Map(%s) ",
                 MOD, meth, tostring(newValue), type(newValue), tostring(ldtMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local binNumber  = 0;
  local key = 0; 
  if ldtMap[M_StoreState] == SS_COMPACT then
    -- In the case of LMAP, we dont need to worry about this
    -- because we never call this for compact
    return 0
  else
    if( ldtMap[M_KeyType] == KT_ATOMIC ) then
      key = newValue;
      GP=F and info(" Type of Key ATOMIC = %s", type(key))
    else
      -- WE ARE DEALING WITH NAME VALUE PAIRS HERE
      -- SO THE KEY WILL BE BASED ON THE STRING OF THE 
      -- THE KEY/NAME FIELD, IF A KEY-FUNCTION IS NOT SPECIFIED  
      local key = getKeyValue( ldtMap, newValue );
    end

    if type(key) == "number" then
      binNumber  = numberHash( key, ldtMap[M_Modulo] );
    elseif type(key) == "string" then
      binNumber  = stringHash( key, ldtMap[M_Modulo] );
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type %s (should be number, string or map)",
           MOD, meth, type(key) );
      error( ldte.ERR_INTERNAL );
    end
  end
  
  local digestlist = ldtMap[M_DigestList]
  GP=E and info("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) Entry : %s",
                 MOD, meth, tostring(newValue), binNumber, tostring(digestlist[binNumber]) );

  return binNumber;
end -- computeSetBin()

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
-- All LDT subrecs have a properties bin that describes the subrec.  This
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

  local subrecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subrecCount + 1;

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
-- Set the values in the LDR subrec's Control Bin map. LDR Records
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

local function initializeSubRec( topRec, ldtBinName, newLdrChunkRecord, ldrPropMap, ldrMap )
  local meth = "initializeSubRec()";
  GP=E and info("[ENTER]: <%s:%s> Name: TopRec: ", MOD, meth );

  local ldtCtrl = topRec[ldtBinName] ;
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  -- topRec's digest is the parent digest for this subrec 
  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  -- Subrec's (its own) digest is the selfDigest :)
  ldrPropMap[PM_SelfDigest]   = record.digest( newLdrChunkRecord ); 
  ldrPropMap[PM_Magic]        = MAGIC;
  ldrPropMap[PM_RecType]   = RT_SUB;
  
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
-- new chunk into the lsoMap (the warm dir list), and return it.
-- In this function, we create a LDR subrec and init two structures: 
-- a. The property-map for the new LDR subrec chunk
-- b. The ctrl-map for the new LDR subrec chunk record
-- a & b are done in initializeSubRec()
-- Once that is done in the called-function, we then make a call to create 
-- an ESR and init that struct as well in createAndInitESR(). 
-- From the above function, we call setLdtRecordType() to do some 
-- byte-level magic on the ESR property-map structure. 
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

  -- Update the subrec count (and remember to save the change)
  local subrecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subrecCount + 1;
  local rc = addSubrecToContext( src, newSubRec ); 
  
  -- Each subrec that gets created, needs to have its properties initialized. 
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
   GP=F and info("[DEBUG]: <%s:%s> Appending NewChunk %s with digest(%s) to DigestList(%s)",
    MOD, meth, tostring(newSubRec), tostring(newChunkDigest), tostring(ldtMap[M_DigestList]));

  GP=F and info("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LMap(%s): ",
    MOD, meth, tostring(newChunkDigest), tostring(ldtMap));
   
  -- Increment the Digest Count
  -- gets inceremented once per LDR entry add. 
  local ChunkCount = ldtMap[M_ListDigestCount]; 
  ldtMap[M_ListDigestCount] = (ChunkCount + 1);

  -- This doesn't appear to be getting set (updated) anywhere else.
  -- Do it here.
  aerospike:update_subrec( newSubRec );

  GP=E and info("[EXIT]: <%s:%s> ldrPropMap(%s) Name-list: %s value-list: %s ",
    MOD, meth, tostring( ldrPropMap ), tostring(newSubRec[LDR_NLIST_BIN]), tostring(newSubRec[LDR_VLIST_BIN]));
  
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
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================

local function ldrInsertList(ldrChunkRec,ldtCtrl,listIndex,nameList,valueList )
  local meth = "ldrInsertList()";
  GP=E and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

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
  GP=F and info(" <%s:%s> Chunk ldrMap is [DEBUG] (%s)", MOD, meth, tostring(ldrMap));
  
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];
    
   GP=F and info(" <%s:%s> Chunk ldr Name-List: %s Value-List: (%s)", MOD, meth, tostring(ldrNameList), tostring(ldrValueList));
   GP=F and info(" <%s:%s> To be inserted Name-List: %s Value-List: (%s)", MOD, meth, tostring(nameList), tostring(valueList));
  
  local chunkNameIndexStart = list.size( ldrNameList ) + 1;
  local chunkValueIndexStart = list.size( ldrValueList ) + 1;
  
  GP=F and info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( nameList ) + 1 - listIndex;
  local itemSlotsAvailable = (ldtMap[M_LdrEntryCountMax] - chunkNameIndexStart) + 1;

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
  -- GP=F and info("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
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
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertBytes( ldrChunkRec, ldtCtrl, listIndex, nameList, valueList )
  local meth = "ldrInsertBytes()";
  GP=E and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

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
-- ldrInsert(ldrChunkRec,ldtCtrl,listIndex,insertList )
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

  GP=E and info("[ENTER]: <%s:%s> list-size(%d) NameList(%s), valueList(%s), ChunkSummary(%s)",
    MOD, meth, listSize, tostring( nameList ), tostring( valueList ), tostring(ldrChunkRec));
    
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  if ldtMap[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,ldtCtrl,listSize,nameList,valueList);
  else
    return ldrInsertBytes(ldrChunkRec,ldtCtrl,listSize,nameList,valueList);
  end

end -- ldrInsert()


local function lmapGetLdrDigestEntry( src, topRec, ldtBinName, entryItem, create_flag)

  local meth = "lmapGetLdrDigestEntry()";
  
  local ldtCtrl = topRec[ldtBinName] ;
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local topLdrChunk = nil; 

  GP=E and info("[ENTER]: <%s:%s> lMap(%s)", MOD, meth, tostring( ldtMap ));
  
  local digest_bin = computeSetBin( entryItem, ldtMap ); 
  local digestlist = ldtMap[M_DigestList]; 
	
  GP=F and info(" <%s:%s> : Digest-entry for this index %d ",
             MOD, meth, digest_bin);
             
  if digestlist == nil then
    -- sanity check 
    warn("[ERROR]: <%s:%s>: Digest list nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
 end 
   	
  GP=F and info(" <%s:%s> !!!!!!! Digest List size : %d list %s", MOD, meth, list.size( digestlist ), tostring(digestlist));
   	
  local newdigest_list = list(); 
  for i = 1, list.size( digestlist ), 1 do
     if i == digest_bin then 
	    
       if digestlist[i] == 0 then 
         -- This is a new unique key, create a chunk  
         GP=F and info(" <%s:%s> : Digest-entry empty for this index %d ",
         MOD, meth, digest_bin);
         GP=F and info("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
         topLdrChunk = subRecCreate( src, topRec, ldtBinName ); -- create new
         ldtMap[M_TopFull] = false; -- reset for next time.
         create_flag = true; 
          
       else 
          -- local newChunkDigest = record.digest( topLdrChunk );
          GP=F and info(" <%s:%s> : Digest-entry valid for this index %d digest(%s)  ",
          MOD, meth, digest_bin, tostring( digestlist[i] ));
          local stringDigest = tostring( digestlist[i] );
          topLdrChunk = openSubrec( src, topRec, stringDigest );
       end
          
     end -- end of digest-bin if, no concept of else, bcos this is a hash :)

  end -- end of for 
  
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth ); 
  return topLdrChunk; 

end --lmapGetLdrDigestEntry()

local function lmapCheckDuplicate(ldtMap, ldrChunkRec, entryItem)
  
  local flag = false; 
  if ldtMap[M_StoreMode] == SM_LIST then
    local ldrValueList = ldrChunkRec[LDR_NLIST_BIN];
    GP=F and info(" Duplicate check list %s", tostring(ldrValueList));
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
end

-- ======================================================================
-- lmapLdrSubRecInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the digest-list
-- dir list -- a directory of Large Data Records that will contain 
-- the data entries.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtCtrl: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function lmapLdrSubRecInsert( src, topRec, ldtBinName, newName, newValue)
  local meth = "lmapLdrSubRecInsert()";
  
  local rc = 0;
  local ldtCtrl =  topRec[ldtBinName] ;
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local DigestList = ldtMap[M_DigestList];
  local digest_flag = false; 
  local topLdrChunk = nil; 
  local create_flag = true;
  
  GP=E and info("[ENTER]: !!!!!Calling <%s:%s> with DL (%s) for Name-Value pair %s:%s !!!!!",
  MOD, meth, tostring(ldtMap[M_DigestList]), tostring( newName ), tostring( newValue ));
    

  -- You have a new entry to be inserted, first go and create the LDR needed 
  -- to hold this listEntry. This also takes care of ldrPropMap and ESR creation. 
   
  local DigestListCopy = ldtMap[M_DigestList];
  
  -- In the name-value pair in lmap, the name acts as the key !!!!!!!!!! 
  -- This function creates a subrec if the entry is empty, returns open chunk 
  topLdrChunk = lmapGetLdrDigestEntry( src, topRec, ldtBinName, newName, create_flag); 
   
  if topLdrChunk == nil then
 	-- sanity check 
    warn("[ERROR]: <%s:%s>: topLdrChunk nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end
  
  local newChunkDigest = record.digest( topLdrChunk );
 
  GP=F and info("[DEBUG]: <%s:%s> LDR chunk Name-list:%s, value-list:%s ",
   MOD, meth, tostring( topLdrChunk[LDR_NLIST_BIN] ),
   tostring( topLdrChunk[LDR_VLIST_BIN] ) );
      
  -- Before we try to do insert, lets take care of duplicates using name/key
  local exists_flag = lmapCheckDuplicate(ldtMap, topLdrChunk, newName); 
  
  if exists_flag == true then
    warn("[INTERNAL ERROR]:<%s:%s> Duplicate Entry %s already exists ",  
           MOD, meth, tostring(entryItem));
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end 
   
  -- HACK : TODO : Fix this number to list conversion  
  local nameList = list(); 
  list.append(nameList, newName); 
  local valueList = list(); 
  list.append(valueList, newValue); 
  
  local totalEntryCount = list.size( nameList );
  GP=F and info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  
  -- Do an ldr insert from index 1 of entryList into topLdrChunk . 
    
  local countWritten = ldrInsert(topLdrChunk, ldtCtrl, 1, nameList, valueList);
  GP=F and info(" !!!!!!! countWritten %d !!!", countWritten);
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end
  
  local itemsLeft = totalEntryCount - countWritten;
  -- removing the retry part of the code to attempt ldrInsert
  -- just print a warning and move-on. 
  if itemsLeft > 0 then 
  	warn("[ERROR]: <%s:%s>: Some items might not be inserted to lmap list-size : %d inserted-items : %d", 
  	      MOD, meth, list.size( entryList ),  itemsLeft);
  end 
  
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount + countWritten; -- Incr # of valid items
  ldtMap[M_TotalCount] = totalCount + countWritten; -- Incr # of total items
  
  
  GP=F and info("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    MOD, meth, ldrChunkSummary( topLdrChunk ));

  GP=F and info("[DEBUG]: <%s:%s> Calling SUB-REC  Update ", MOD, meth );
  if src == nil then 
  	GP=F and info("[DEBUG]: <%s:%s> SRC NIL !!!!!!1 ", MOD, meth );
  end
  rc = updateSubrec( src, topLdrChunk, newChunkDigest );
  GP=F and info("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",MOD,meth, tostring(status));
  GP=F and info("[DEBUG]: <%s:%s> Calling SUB-REC  Close ", MOD, meth );

  -- Close ALL of the subrecs that might have been opened
  rc = closeAllSubrecs( src );
  GP=F and info("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));
    
  -- This is the part where we take the LDR list we've built and add it to the 
  -- digest list. 
  -- TODO : This needs to be moved to a separate function. 
  -- TODO : create_flag is WIP for now. Needs to be fixed later-on
  if create_flag == true then  
    local digest_bin = computeSetBin( newName, ldtMap ); 
    local digestlist = ldtMap[M_DigestList]; 
    
    if digestlist == nil then
      -- sanity check 
      warn("[ERROR]: <%s:%s>: Digest list nil or empty", MOD, meth);
      error( ldte.ERR_INTERNAL );
    end 
    
    local newdigest_list = list(); 
    for i = 1, list.size( digestlist ), 1 do
        if i == digest_bin then 
          if digestlist[i] == 0 then
            GP=F and info(" <%s:%s> Appending digest-bin %d with digest %s for value :%s ",
                 MOD, meth, digest_bin, tostring(newChunkDigest),
                 tostring(entryItem) ); 
             GP=F and info(" !!!!!!! Digest-entry empty, inserting !!!! ");
             list.append( newdigest_list, newChunkDigest );
          else
             GP=F and info("<><> Digest-entry index exists, skip DL touch");
             list.append( newdigest_list, digestlist[i] );
          end
        else
          list.append( newdigest_list, digestlist[i] );
        end -- end of digest_bin if 
    end -- end of for-loop 
    
    ldtMap[M_DigestList] = newdigest_list; 
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

    rc = aerospike:update( topRec );

    if( rc == nil or rc == 0 ) then
      GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 

  end -- end of create-flag 
       
  GP=E and info("[EXIT]: !!!!!Calling <%s:%s> with DL (%s) for %s !!!!!",
  MOD, meth, tostring(ldtMap[M_DigestList]), tostring( entryItem ));
  local digestlist = ldtMap[M_DigestList]; 
  GP=F and info(" DigestList %s Size: %s", tostring(digestlist), tostring(list.size(digestlist)));
  
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
  
  return rc;
 end -- lmapLdrSubRecInsert

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

local function simpleScanList(topRec, ldtBinName, resultMap, newName, newValue, 
       flag, userModule, filter, fargs )

  local meth = "simpleScanList()";

  GP=E and info("[ENTER]: <%s:%s> Name-List(%s), Value-List(%s)",
                 MOD, meth, tostring(nameList), 
                 tostring(valueList));
                 
  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  
  local rc = 0;

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 

  -- In LMAP with name-value pairs, all operations are done 
  -- based on key-comparison. So we will parse name-list 
  -- to do everything !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  local resultValue = nil;

  for i = 1, list.size( nameList ), 1 do
    GP=F and trace("[DEBUG]<%s:%s> Comparing Name-entry(%s) with Name-list(%s)",
                 MOD, meth, tostring(newName), tostring(nameList[i]));

    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
    if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
      -- CHECK IF THE KEY/NAME IS PRESENT, transform function applicable
      -- only to value  
      resultValue = unTransformSimpleCompare(nil, nameList[i], newName);
      if resultValue ~= nil then

        GP=E and info("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));

        -- Return point for FV_DELETE and FV_SCAN, what should we return here ?
        -- if its a search, the result should return the pair in the
        -- name-value pair. 
        -- If its a deleted, the result should be just the return code and the 
        -- result-list will be nil !!!

        if( flag == FV_DELETE ) then
          -- local newString = nameList[i]..":"..valueList[i];  
          -- list.append( resultList, newString );
          resultMap[nameList[i]] =  valueList[i];

          nameList[i] = FV_EMPTY; -- the name-entry is NO MORE
          valueList[i] = FV_EMPTY; -- the value-entry is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          ldtCtrl[1] = propMap; 
          ldtMap[M_CompactNameList] = nameList; 
          ldtMap[M_CompactValueList] = valueList; 
          ldtCtrl[2] = ldtMap;
          topRec[ldtBinName] = ldtCtrl; 
          return 0 -- show caller nothing got inserted, this is a delete (don't count it)
        elseif flag == FV_INSERT then
	  -- Duplicate check for insertions 
          warn("[INTERNAL ERROR]:<%s:%s> Duplicate Entry. This name %s already exists ",  
                MOD, meth, tostring(newName));
          error( ldte.ERR_BIN_ALREADY_EXISTS );
        elseif flag == FV_SCAN then
          -- APPLY FILTER ON THE VALUE
          -- In the name:value pair version of LMAP, we dont send in any value, only the name
          -- but we still need to apply filter to the return, so we'll use this vague call 
          -- with both the params being the same.   
          resultValue = unTransformSimpleCompare(unTransform, valueList[i], valueList[i]);
          GP=F and info(" FV_SCAN resultValue: %s", tostring(resultValue));  
          local resultFiltered;
	  if filter ~= nil and fargs ~= nil then
         	resultFiltered = functionTable[filter]( resultValue, fargs );
    	  else
      		resultFiltered = resultValue;
    	  end
          -- local newString = nameList[i]..":"..resultFiltered; 
          -- list.append( resultList, newString );
          resultMap[nameList[i]] = resultFiltered;
          return 0; -- Found it. Return with success.
        end -- end of flag-type check 
      end -- end resultValue check 
    end -- end if not null and not empty
  end -- end for each item in the list

  -- Didn't find it.  If FV_INSERT, then append the name and value to the list
  -- Ideally, if we noticed a hole, we should use THAT for insert and not
  -- make the list longer.
  -- TODO: Fill in holes if we notice a lot of gas in the lists.

  if flag == FV_INSERT then
    GP=E and info("[EXIT]: <%s:%s> Inserting(%s)",
                   MOD, meth, tostring(newValue));
    local storeValue = applyTransform( transform, newValue );
    list.append( valueList, storeValue );
    list.append( nameList, newName );
    return 1 -- show caller we did an insert
  end

  GP=F and info("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
                 MOD, meth, tostring(value));
  return 0; -- All is well.
end -- simpleScanList

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
local function complexScanList( topRec, ldtBinName, resultMap, newName,
    newValue, flag, userModule, filter, fargs )

  local meth = "complexScanList()";
  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  
  GP=E and info("[ENTER]: <%s:%s> Name-List(%s), Value-List(%s)",
                 MOD, meth, tostring(nameList), 
                 tostring(valueList));
  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  
  if ldtMap[M_Transform] ~= nil then
    transform = functionTable[ldtMap[M_Transform]];
  end

  if ldtMap[M_UnTransform] ~= nil then
    unTransform = functionTable[ldtMap[M_UnTransform]];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 

  -- In LMAP with name-value pairs, all operations are done 
  -- based on key-comparison. So we will parse name-list 
  -- to do everything !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  local resultValue = nil;

  for i = 1, list.size( nameList ), 1 do
    GP=F and info("[DEBUG]<%s:%s> Comparing Name-entry(%s) with Name-list(%s)",
                   MOD, meth, tostring(newName), tostring(nameList[i]));

    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
    if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
      -- CHECK IF THE KEY/NAME IS PRESENT, transform function applicable only to value  
      resultValue = unTransformComplexCompare(ldtMap, nil, nameList[i], newName);
      if resultValue ~= nil then

        GP=E and info("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));

	-- Return point for FV_DELETE and FV_SCAN, what should we return here ?
        -- if its a search, the result should return the pair in the name-value pair
	-- if its a deleted, the result should be just the return code and the 
        -- result-list will be nil !!!!!!!!!!!!!!!!!!!!

        if( flag == FV_DELETE ) then
          -- local newString = nameList[i]..":"..valueList[i];  
          -- list.append( resultList, newString );
          resultMap[nameList[i]] = valueList[i];

          nameList[i] = FV_EMPTY; -- the name-entry is NO MORE
          valueList[i] = FV_EMPTY; -- the value-entry is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          ldtCtrl[1] = propMap; 
          ldtMap[M_CompactNameList] = nameList; 
          ldtMap[M_CompactValueList] = valueList;
          ldtCtrl[2] = ldtMap;
          topRec[ldtBinName] = ldtCtrl; 
          return 0 -- show caller nothing got inserted, this is a delete (don't count it)
        elseif flag == FV_INSERT then
	  -- Duplicate check for insertions 
          warn("[INTERNAL ERROR]:<%s:%s> Duplicate Entry. This name %s already exists ",  
                MOD, meth, tostring(newName));
          error( ldte.ERR_BIN_ALREADY_EXISTS );
          return 0 -- show caller there is a duplicate (don't count it)
        elseif flag == FV_SCAN then
          -- APPLY FILTER ON THE VALUE 
          -- In the name:value pair version of LMAP, we dont send in any value, only the name
          -- but we still need to apply tranform if provided, so we'll use this vague call 
          -- with both the params being the same.   
          resultValue = unTransformComplexCompare(ldtMap, unTranform, valueList[i], valueList[i]);
          GP=F and info(" FV_SCAN resultValue: %s", tostring(resultValue));  
           
          local resultFiltered;
	  if filter ~= nil and fargs ~= nil then
         	resultFiltered = functionTable[filter]( resultValue, fargs );
    	  else
      		resultFiltered = resultValue;
    	  end
          -- local newString = nameList[i]..":"..resultFiltered; 
          -- list.append( resultList, newString );
          resultMap[nameList[i]] = valueList[i];

          return 0; -- Found it. Return with success.
        end -- end of flag-type check 
      end -- end resultValue check 
    end -- end if not null and not empty
  end -- end for each item in the list

  -- Didn't find it.  If FV_INSERT, then append the name and value to the list
  -- Ideally, if we noticed a hole, we should use THAT for insert and not
  -- make the list longer.
  -- TODO: Fill in holes if we notice a lot of gas in the lists.

  if flag == FV_INSERT then
    GP=E and info("[EXIT]: <%s:%s> Inserting(%s)",
                   MOD, meth, tostring(value));
    local storeValue = applyTransform( transform, newValue );
    list.append( valueList, storeValue );
    list.append( nameList, newName );
    return 1 -- show caller we did an insert
  end

  GP=F and info("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
                 MOD, meth, tostring(value));
  return 0; -- All is well.
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) resultMap is nil when called for insertion 
-- (*) ldtCtrl: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( topRec, ldtBinName, resultMap, newName, newValue, 
       flag, userModule, filter, fargs )
  local meth = "scanList()";
  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];

  GP=F and trace("[DEBUG]<%s:%s> Key-Type(%s)",
    MOD, meth, tostring(ldtMap[M_KeyType]));

  -- Set up the functions for UnTransform and Filter.
  setReadFunctions( ldtMap, userModule, filter, fargs );

  if ldtMap[M_KeyType] == KT_ATOMIC then
    return simpleScanList(topRec,ldtBinName,resultMap,newName,newValue,flag);
  else
    return complexScanList(topRec,ldtBinName,resultMap,newName,newValue,flag);
  end
end -- scanList()

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
  if( position > 0 ) then
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
-- (*) src: subrecContext: The pool of open subrecords
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
  local nodeRec = aerospike:create_subrec( topRec );
  if( nodeRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating Subrec", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

local rc = initializeNode( topRec, nodeRec, ldtCtrl );
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
-- || subRecInsert
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
local function subRecInsert( topRec, ldtCtrl, newName, newValue )
  local meth = "subRecInsert()";
  GP=E and trace("[ENTER]<%s:%s> Name(%s) Value(%s)",
   MOD, meth, tostring(newName), tostring(newValue));
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local rc = 0; -- start out OK.

  local binNumber = computeSetBin( searchName, ldtMap );
  local hashDirectory = ldtMap[M_DigestList];
  local hashCell = hashDirectory[binNumber];
  local subRec;
  local src = createSubrecContext();

  -- Maybe, eventually, we'll allow a few items to be stored directly
  -- in this directory (to save the SUBREC management for small numbers).
  -- TODO: Add ability to hold small lists -- and read them -- soon.

  -- If no subrecord, create one
  if( hashCell == nil or hashCell == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> Cell(%d) Empty: Creating Subrec",
      MOD, meth, binNumber);
    subRec = subRecCreate( src, topRec, ldtCtrl );
  else
    -- We have a subrec -- open it
    local digestString = tostring(hashCell);
    local subRec = openSubrec( src, topRec, digestString );
    if( subrec == nil ) then
      warn("[ERROR]: <%s:%s>: subrec nil or empty: Digest(%s)",  MOD, meth,
        digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
  end

  local nameList = subRec[LDR_NLIST_BIN];
  local valueList = subRec[LDR_VLIST_BIN];

  if( nameList == nil or valueList == nil ) then
    warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  local position = searchList( ldtCtrl, nameList, newName );
  if( position > 0 ) then
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
end -- function subRecInsert()

-- ======================================================================
-- localInsert( topRec, ldtBinName, newName, newValue, 1 );
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- !!!!!!!! IN LMAP THIS IS CALLED ONLY IN SS_COMPACT MODE !!!!!!!!!
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtBinName: The LMap control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, ldtBinName, newName, newValue, stats )
 
  local meth = "localInsert()";
    
  GP=E and info("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));
  
  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  local insertResult = 0;
  
  if nameList == nil or valueList == nil then
    warn("[ERROR]:<%s:%s> Name/Value is nil: BinName(%s)",
                 MOD, meth, tostring( ldtBinName ) );
    error( ldte.ERR_INTERNAL );
  else
    GP=F and trace("[DUMP]:<%s:%s> BinName(%s) NameList(%s) ValList(%s)",
      MOD, meth, tostring( ldtBinName ), tostring(nameList),tostring(valList));

    -- Look for the value, and insert if it is not there.
    insertResult =
      scanList( topRec, ldtBinName, nil, newName, newValue, FV_INSERT, nil, nil );
  end
                
  -- update stats if appropriate.
  -- The following condition is true only for FV_INSERT returning a success

  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = ldtMap[M_TotalCount];
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    ldtMap[M_TotalCount] = totalCount + 1; -- Total number of items goes up
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  end
 
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
  else
    warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
  end 
  GP=E and info("[EXIT]: <%s:%s>Storing Record() with New Value(%s): List(%s)",
                 MOD, meth, tostring( newValue ), tostring( binList ) );
  -- No need to return anything
end -- localInsert


-- ======================================================================
-- rehashSetToLmap( src, topRec, ldtBinName,  newName, newValue );
-- ======================================================================
-- This is a variation of the standard rehashSet present in LSET. This is the 
-- puece of code that actually converts a compact-mode LSET into a fixed-size 
-- warm-list and makes it a LMAP. Find detailed notes in lmap_design.lua
--  
-- Traditional RehashSet (as present in LSET): 
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- 
-- Enhanced rehashSetToLmap (as used in LMAP)
-- When the number of items stored in a simple compact-mode LSET-like 
-- structure exceeds the threshold specified in the control-map, we do the 
-- following in-order: 
-- a. Copy the existing list into a temp-list
-- b. Add lmap related control-fields to ldtMap 
-- c. Build the subrec structure needed to add a list of digests
-- (fixed-size warm-list) 
-- d. Insert records and shove into subrecs appropriately
-- 
-- 
-- d. Add ESR 
-- e. Call subrec 
-- f. Move the current set of records into 1 warm-list structure 
-- g. Update ctrlinfo params accordingly  
-- Parms:
-- (*) topRec
-- (*) ldtBinName
-- (*) lsetCtrlMap
-- ======================================================================
local function rehashSetToLmap( src, topRec, ldtBinName,  newName, newValue )
  local meth = "rehashSetToLmap()";
  GP=E and info("[ENTER]:<%s:%s> !!!! REHASH !!!! Name: %s Src %s, Top: %s, Ctrl: %s, Name: %s Val : %s", 
		 MOD, meth, tostring(ldtBinName),tostring(src),tostring(topRec),tostring(ldtMap), tostring(newName), tostring(newValue));

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  -- If we are calling rehashSet, we probably have only one LSET list which we
  -- can access directly with name as all LMAP bins are yser-defined names. 
  local ldtCtrl =  topRec[ldtBinName];
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local nameList = ldtMap[M_CompactNameList]; 
  local valueList = ldtMap[M_CompactValueList]; 
  -- local singleBinList = ldtMap[M_CompactList];
  if nameList == nil or valueList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(ldtBinName));
    error( ldte.ERR_INSERT );
  end
  
  -- Copy existing elements into temp list
  local listNameCopy = list.take(nameList, list.size( nameList ));
  local listValueCopy = list.take(valueList, list.size( valueList ));
  ldtMap[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
 
  -- create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  
  GP=E and info("[ENTER]:<%s:%s> Calling initializeLMapRegular ", MOD, meth );
  initializeLMapRegular(topRec, ldtBinName); 
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = ldtMap[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- empty list created during initializeLmap()
    list.append( ldtMap[M_DigestList], 0 );
  
  end -- for each new bin
  
  -- take-in the new element whose insertion request has triggered the rehash. 
  
  GP=F and info("%s:%s Before calling the first subrec-insert Name-list %s, Value-list %s ", MOD, meth, tostring(listNameCopy), tostring(listValueCopy) );
  list.append(listNameCopy, newName);
  list.append(listValueCopy, newValue);
  
  -- Before calling code to rehash and create-subrecs, reset COMPACT mode settings: 
  ldtMap[M_CompactNameList] = nil; 
  ldtMap[M_CompactValueList] = nil; 
  propMap[PM_ItemCount] = 0;
  -- TotalCount is the count of all elements including deletions. Technically these are not getting deleted. so we'll reset
  -- TODO : Add TotalCount math to deletions  !!! 
  ldtMap[M_TotalCount] = 0; 

  for i = 1, list.size(listNameCopy), 1 do
      -- Now go and create the subrec structure needed to insert a digest-list
	  -- Subtle change between LSET and LMAP rehash: In the case of LSET rehash, 
	  -- we created M_Modulo LSET-bins and inserted existing Bin-0 elemnts across
	  -- all the N bins. In the case of LMAP, this now becomes a digest-list of 
	  -- entries, so we take Bin-0 elements (called by ldtBinName and not Bin-0)
	  -- and insert one LDR chunk with digest-entry. 
	  
	  -- This function does the following : 
	  -- Create and init subrec if needed
	  -- Create and init ESR if needed 
	  -- set record ldt type and prop-map
	  -- Insert existing lset list (listCopy param) items into digest list 
	  -- update top-rec, record prop-map etc 
	  -- return result. So we dont need to call localInsert() for this case
  	  lmapLdrSubRecInsert( src, topRec, ldtBinName, listNameCopy[i], listValueCopy[i] ); 
  end
 
  
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth );
end -- rehashSetToLmap()


-- ======================================================================
-- ======================================================================
local function lmapInsertRegular( topRec, ldtBinName, newName,  newValue) local meth = "lmapInsertRegular()";
  
  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local totalCount = ldtMap[M_TotalCount];

  GP=F and info("!!!!!!: <%s:%s> ListMode : %s value %s ThreshHold : %s!!!!!!!! ", MOD, meth, tostring( ldtMap[M_StoreState] ), tostring( newValue ), tostring(ldtMap[M_ThreshHold]));
  
   -- we are now processing insertion for a new element and we notice that 
   -- we've reached threshold. Excellent ! 
   -- so now, lets go and do a rehash-first and also follow-up with an 
   -- insertion for the new element. 
   
  local src = createSubrecContext();
  if ldtMap[M_StoreState] == SS_COMPACT and
      totalCount == ldtMap[M_ThreshHold]    
  then
    -- !!! Here we are switching from compact to regular mode !!!
    -- refer to lmap_design.lua for functional notes 
    GP=F and info("!!!!!!: <%s:%s> ListMode : %s !!!!!!!! ", MOD, meth, tostring( ldtMap[M_StoreState] ));
    rehashSetToLmap( src, topRec, ldtBinName,  newName, newValue );
  else
      GP=F and info("!!!!!!: <%s:%s>  ListMode : %s Direct-call %s!!!!!!!! ", MOD, meth, tostring( ldtMap[M_StoreState] ), tostring(newValue) );
      lmapLdrSubRecInsert( src, topRec, ldtBinName, newName, newValue); 
  end
   
  GP=E and info("[EXIT]: <%s:%s>", MOD, meth );
  
end -- lmapInsertRegular()

-- ======================================================================
-- ======================================================================
local function localLMapCreate( topRec, ldtBinName, createSpec )
  local meth = "localLMapCreate()";
  
  GP=E and info("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring(createSpec) );
                 
  if createSpec == nil then
    GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s) NULL createSpec",
      MOD, meth, tostring(ldtBinName));
  else
    GP=E and info("[ENTER2]: <%s:%s> ldtBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( createSpec ));
  end

  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if the Bin Name is bad.
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

  -- Set up a new LDT Bin
  setupLdtBin( topRec, ldtBinName, createSpec );
----
----
---- Some simple protection of faulty records or bad bin names
--  -- flag set to false because we need not check for ctrl bins. 
--  validateRecBinAndMap( topRec, ldtBinName, false );
--
--  -- Check to see if Set Structure (or anything) is already there,
--  -- and if so, error.  We don't check for topRec already existing,
--  -- because that is NOT an error.  We may be adding an LMAP field to an
--  -- existing record.
--  if( topRec[ldtBinName] ~= nil ) then
--    GP=E and warn("[ERROR EXIT]: <%s:%s> LMAP CONTROL BIN Already Exists",
--                   MOD, meth );
--    error( ldte.ERR_BIN_ALREADY_EXISTS );
--  end
--  
--  GP=F and info("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
--  local ldtCtrl = initializeLMap( topRec, ldtBinName);
--  local propMap = ldtCtrl[1]; 
--  local ldtMap = ldtCtrl[2]; 
--  
--  -- Set the type of this record to LDT (it might already be set)
--  record.set_type( topRec, RT_LDT ); -- LDT Type Rec
--  
--  -- If the user has passed in some settings that override our defaults
--  -- (createSpec) then apply them now.
--  if createSpec ~= nil then 
--    adjustLMapCtrlInfo( ldtMap, createSpec );
--    -- Changes to the map need to be re-appended to topRec  
--    GP=F and info(" After adjust Threshold : %s ", tostring( ldtMap[M_ThreshHold] ) );
--    topRec[ldtBinName] = ldtCtrl;
--    record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
--    
--    GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after adjustLMapCtrlInfo(%s)",
--       MOD, meth , ldtSummaryString(ldtCtrl));
--  end
--
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );

  GP=E and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- end of localLMapCreate

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || AS Large Map Insert (with and without Create)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Insert a value into the MAP.
-- Take the value, perform a hash and a modulo function to determine which
-- bin list is used, then add to the list.
-- We will use user-given BIN names for this initial prototype
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
-- Please refer to lmap_design.lua for further notes. 
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- ======================================================================
local function
localLMapInsert( topRec, ldtBinName, newName, newValue, createSpec )
  local meth = "localLMapInsert()";
   
  GP=E and trace("[ENTRY]<%s:%s> Bin(%s) name(%s) value(%s) module(%s)",
    MOD, meth, tostring(ldtBinName), tostring(newName),tostring(newValue),
    tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    info("[Notice] <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 

  -- Set up the Read/Write Functions (KeyFunction, Transform, Untransform)
  setReadFunctions( ldtMap, nil, nil, nil );
  setWriteFunctions( ldtMap );
  
  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local totalCount = ldtMap[M_TotalCount];

  -- In the case of LMAP, we call localInsert only if it is SS_COMPACT mode
  -- insertion of elements into the first LMAP bin like an lset-insert. If not
  -- rehashSettoLmap will take care of the insertion as well. Please refer to
  -- notes mentioned in rehashSettoLmap() about these differences. 

  if ldtMap[M_StoreState] == SS_COMPACT and totalCount < ldtMap[M_ThreshHold]
  then
    compactInsert( ldtCtrl, newName, newValue );
    -- we are in compact mode
    -- GP=F and info("localInsert() for LMAP INSERT Count %d Threshold : %d ",
    			-- totalCount, tostring( ldtMap[M_ThreshHold] ) );
    -- localInsert( topRec, ldtBinName, newName, newValue, 1 );
  else
    subRecInsert( topRec, ldtCtrl, newName, newValue); 
    -- lmapInsertRegular( topRec, ldtBinName, newName, newValue); 
  end

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
end -- function localLMapInsert()

-- ======================================================================
-- ldrDeleteList( topLdrChunk, ldtCtrl, listIndex,  insertList, filter, fargs )
-- ======================================================================
-- Insert (append) the LIST of values pointed to from the digest-list, 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) ldtCtrl: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) entryList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================

local function ldrDeleteList(topRec, ldtBinName, ldrChunkRec, listIndex,
  entryList, userModule, filter, fargs)
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
    -- Nothing to be deleted in this subrec
    GP=E and info("[ENTER]: <%s:%s> Nothing to be deleted in this subrec !!",
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
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: LDR list is shorter than deletion list(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end
 
  -- Basically, crawl thru the list, copy-over all except our item to the new list
  -- re-append back to ldrmap. Easy !
  
  GP=F and info("!!!![DEBUG]:<%s:%s>:ListMode: Before deletion Value List %s !!!!!!!!!!",
     MOD, meth, tostring( ldrValueList ) );
 
  local NewldrNameList = list(); 
  local NewldrValueList = list(); 
  local num_deleted = 0; 
  GP=F and info(" BeforeDelete Name & Value %s %s", tostring(ldrNameList), tostring(ldrValueList));
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
  GP=F and info(" AfterDelete Name & Value %s %s", tostring(ldrNameList), tostring(ldrValueList));
 
  -- Update subrec 
  local rc = aerospike:update_subrec( ldrChunkRec );
  if( rc == nil or rc == 0 ) then
      -- Close ALL of the subrecs that might have been opened
      GP=F and info("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",MOD,meth, tostring(rc));
  else
     warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
     error( ldte.ERR_SUBREC_UPDATE );
  end

  local num_deleted = totalListSize - list.size( ldrChunkRec[LDR_NLIST_BIN] ); 
  GP=F and info(" Delete : Num-deleted :%s", tostring(num_deleted));  
  local itemCount = propMap[PM_ItemCount];
  local totalCount = ldtMap[M_TotalCount];
  propMap[PM_ItemCount] = itemCount - num_deleted; -- number of valid items goes down
  ldtMap[M_TotalCount] = totalCount - num_deleted; -- Total number of items goes down 
  
  GP=F and info(" Delete : Num-deleted :%s Mapcount %s", tostring(num_deleted), tostring(propMap[PM_ItemCount])); 
 
  -- Now go and fix the digest-list IF NEEDED 
  -- refer to lmap_design.lua to determine what needs to be done here.
  -- we deleted the one and only (or last) item in the LDR list. 
  if totalListSize == totalItemsToDelete and list.size( ldrChunkRec[LDR_NLIST_BIN] ) == 0 then
    GP=F and info("[DEBUG] !!!!!!!!! Entire LDR list getting Deleted !!!!!!");
    local digestlist = ldtMap[M_DigestList]; 
    GP=F and info(" Digest %s to List we are comapring with %s", tostring(self_digest), tostring(digestlist));
    for i = 1, list.size( digestlist ), 1 do
      if tostring(digestlist[i]) == tostring(self_digest) then 
        GP=F and info("[DEBUG] !! Found matching digest-list Delete Index %d !!", i);
   	GP=F and info("List BEFORE reset Delete: %s", tostring(digestlist))
        GP=F and info("[DEBUG] !! Resetting Delete digest-entry %s to zero !!",
   		         tostring( digestlist[i] ) );
   	digestlist[i] = 0; 
   	GP=F and info("List AFTER Delete reset : %s", tostring(digestlist))
      end 
    end -- end of for loop 
  
   -- update TopRec ()
   ldtMap[M_DigestList] = digestlist; 
   
 end -- end of if check for digestlist reset 
   
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
     GP=E and info("[EXIT]: <%s:%s>", MOD, meth );      
  else
     warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
     error( ldte.ERR_SUBREC_UPDATE );
  end 
   
 return num_deleted;
end -- ldrDeleteList()

-- ==========================================================================
-- ==========================================================================
local function localLMapDelete( topRec, ldtBinName, searchValue,
                          userModule, filter, fargs )
  local meth = "localLMapDelete()";
                            
  GP=E and info("[ENTER]:<%s:%s> Bin-Name(%s) Delete-Value(%s) ",
        MOD, meth, tostring(ldtBinName), tostring(searchValue));      
         
  local resultMap = map(); -- add results to this list.
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local index = 0; 

  -- Set up the Read functions (filter, unTransform)
  setReadFunctions( ldtMap, userModule, filter, fargs );
  
  if ldtMap[M_StoreState] == SS_COMPACT then 
    -- local binList = ldtMap[M_CompactList];
    -- Fow now, scanList() will only NULL out the element in a list, but will
    -- not collapse it.  Later, if we see that there are a LOT of nil entries,
    -- we can RESET the set and remove all of the "gas".
    
    rc = scanList(topRec, ldtBinName, resultMap, searchValue, nil, FV_DELETE, filter, fargs);
    -- If we found something, then we need to update the bin and the record.
    if rc == 0 and map.size( resultMap ) > 0 then
      -- We found something -- and marked it nil -- so update the record
      -- ldtMap[M_CompactList] = binList;
      rc = aerospike:update( topRec );
      if( rc < 0 ) then
        error( ldte.ERR_DELETE );
      end
    elseif rc == 0 and map.size( resultMap ) == 0 then 
      -- This item does not exist
      -- return a not-found error  
      error( ldte.ERR_DELETE );
    end
	  
    return rc;
  else
  	-- we are in regular mode !!! 
  	GP=E and info("[ENTER]:<%s:%s> Doing LMAP delete in regular mode ",
        MOD, meth );
  	
    local digestlist = ldtMap[M_DigestList]; 
  	
  	GP=F and info(" DigestList %s Size: %s",
      tostring(digestlist), tostring(list.size(digestlist)));
  	
  	-- First obtain the hash for this entry
  	local digest_bin = computeSetBin( searchValue, ldtMap );  	
	
        -- sanity check for absent entries 
	if  digestlist[digest_bin] == 0 then 
	  warn("[ERROR]: <%s:%s>: Digest-List index is empty for this value %s ",
        MOD, meth, tostring(searchValue));
      error( ldte.ERR_INTERNAL );
	end 
	
	local stringDigest = tostring( digestlist[digest_bin] );
	local src = createSubrecContext();
    local IndexLdrChunk = openSubrec( src, topRec, stringDigest );
   	
	if IndexLdrChunk == nil then
 	  -- sanity check 
      warn("[ERROR]: <%s:%s>: IndexLdrChunk nil or empty", MOD, meth);
      error( ldte.ERR_INTERNAL );
    end
    
    local ldrMap = IndexLdrChunk[LDR_CTRL_BIN];
    local ldrValueList = IndexLdrChunk[LDR_VLIST_BIN];
    local ldrNameList = IndexLdrChunk[LDR_NLIST_BIN];

    GP=F and info("[DEBUG]: <%s:%s> !!!!!!!!!! NList(%s) VList(%s)",
             MOD, meth, tostring(ldrNameList), tostring( ldrValueList ));

    
    local delChunkDigest = record.digest( IndexLdrChunk );
    
    GP=F and info("!!!!!!!!! Find match digest value: %s",
      tostring(delChunkDigest));
    
    -- HACK : TODO : Fix this number to list conversion  
    local entryList = list(); 
    list.append(entryList, searchValue); 
  
    local totalEntryCount = list.size( entryList );
    GP=F and info("[DEBUG]: <%s:%s> Calling ldrDeleteList: List(%s) Count: %s",
      MOD, meth, tostring( entryList ), tostring(totalEntryCount));
  
     -- The magical function that is going to fix our deletion :)
    local num_deleted =
      ldrDeleteList(topRec, ldtBinName, IndexLdrChunk, 1, entryList );
    
    if( num_deleted == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Chunk Delete", MOD, meth);
      error( ldte.ERR_DELETE );
    end
  
    rc = closeAllSubrecs( src );
    local itemsLeft = totalEntryCount - num_deleted;

     if itemsLeft > 0 then  
       warn("[ERROR]: <%s:%s>: Some items might not have been deleted from lmap list-size : %d deleted-items : %d", 
            MOD, meth, list.size( entryList ),  itemsLeft);
      end 
    GP=F and info("[DEBUG]: <%s:%s> Chunk Summary before storage(%s) Digest-List %s ",
    MOD, meth, ldrChunkSummary( IndexLdrChunk ), tostring(ldtMap[M_DigestList]));
    return 0; 	  

  end -- end of regular mode deleteion 

end -- localLMapDelete()

-- ==========================================================================
-- ==========================================================================
local function ldrSearchList(topRec, ldtBinName, resultMap, ldrChunkRec,
                listIndex, entryList )

  local meth = "ldrSearchList()";
  GP=E and info("[ENTER]<%s:%s> Index(%d) List(%s)",
           MOD, meth, listIndex, tostring( entryList ) );

  local ldtCtrl = topRec[ldtBinName]; 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2];
  local self_digest = record.digest( ldrChunkRec ); 

  -- These 2 get assigned in subRecCreate() to point to the ctrl-map. 
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];

  if ldrNameList == nil then 
    -- Nothing to be searched for in this subrec
    return -1; 
  end 

  GP=F and info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) NList: %s VList(%s)",
           MOD, meth, tostring( ldrMap ), tostring(ldrNameList), tostring( ldrValueList ));

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
  
  GP=F and info("[DEBUG]: <%s:%s> TotalItems(%d) ListSize(%d) Things-tobe-searched : %s",
    MOD, meth, totalItemsToSearch, totalListSize, tostring(entryList) );
    
  if totalListSize < totalItemsToSearch then
  	-- TODO : Check with Toby about this condition 
  	-- also applicable to deletes in regular mode 
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: LDR list is shorter than Search list(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end
 
  -- Basically, crawl thru the list, copy-over all except our item to the new list
  -- re-append back to ldrmap. Easy !
  
  GP=F and info("!!!![DEBUG]:<%s:%s>:ListMode:  Search target list %s !!!!!!!!!!",
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
       
  -- Nothing else to be done for search, no toprec/subrec updates etc 
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
local function simpleScanListAll(topRec, ldtBinName, resultMap, filter, fargs) 

  local meth = "simpleScanListAll()";
  GP=E and info("[ENTER]: <%s:%s> Appending all the elements of List ",
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

  GP=F and info(" Parsing through :%s ", tostring(ldtBinName))

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

  GP=E and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
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
simpleDumpListAll(topRec, resultMap, ldtCtrl, ldtBinName, filter, fargs) 

  local meth = "simpleDumpListAll()";
  GP=E and info("[ENTER]: <%s:%s> Appending all the elements of List ",
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
--    GP=F and info(" Parsing through :%s ", tostring(ldtBinName))
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
--  GP=E and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
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
  GP=E and info("[ENTER]: <%s:%s> Appending all the elements of List ",
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

  GP=F and info(" Parsing through :%s ", tostring(ldtBinName))

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

  GP=E and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
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
local function
complexDumpListAll(topRec, resultMap, ldtCtrl, ldtBinName, filter, fargs) 
  local meth = "complexDumpListAll()";
  GP=E and info("[ENTER]: <%s:%s> Appending all the elements of List ",
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
--    GP=F and info(" Parsing through :%s ", tostring(ldtBinName))
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
-- GP=E and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
--                 MOD, meth, listCount)
--
--  return 0; 
--
end -- complexDumpListAll

-- =======================================================================
-- =======================================================================
local function localLMapSearchAll(topRec,ldtBinName,userModule,filter,fargs)
  local meth = "localLMapSearchAll()";
  rc = 0; -- start out OK.
  GP=E and info("[ENTER]: <%s:%s> Bin-Name: %s Search for Value(%s)",
                 MOD, meth, tostring(ldtBinName), tostring( searchValue ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local resultMap = map();

  -- Set up the Read Functions (UnTransform, Filter)
  setReadFunctions( ldtMap, userModule, filter, fargs );

  if ldtMap[M_StoreState] == SS_COMPACT then 
    -- Find the appropriate bin for the Search value
    GP=F and info(" !!!!!! Compact Mode LMAP Search Key-Type: %s !!!!!",
      tostring(ldtMap[M_KeyType]));
    -- local binList = ldtMap[M_CompactList];
	  
    if ldtMap[M_KeyType] == KT_ATOMIC then
      rc = simpleScanListAll(topRec, ldtBinName, resultMap );
    else
      rc = complexScanListAll(topRec, ldtBinName, resultMap );
    end
	
    GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
    -- HACK : TODO : Fix this number to list conversion  
    local digestlist = ldtMap[M_DigestList];
    local src = createSubrecContext();
	
    -- for each digest in the digest-list, open that subrec, send it to our 
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
          0, entryList, filter, fargs );
        if( rc == nil or rc == 0 ) then
       	  GP=F and info("AllSearch returned SUCCESS %s", tostring(ldrlist));
          break;
         end -- end of if-else check 
         rc = closeSubrec( src, stringDigest )
       end -- end of digest-list if check  
     end -- end of digest-list for loop 
     -- Close ALL of the subrecs that might have been opened
     rc = closeAllSubrecs( src );
  end -- end of else 
  	  
  return resultMap;
end -- end of localLMapSearchAll

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
-- || subRecSearch
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
local function subRecSearch(topRec, ldtCtrl, searchName, resultMap )
  local meth = "subRecSearch()";

  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local rc = 0; -- start out OK.

  local binNumber = computeSetBin( searchName, ldtMap );
  local hashDirectory = ldtMap[M_DigestList];
  local hashCell = hashDirectory[binNumber];

  -- If no subrecord, then we're done
  if( hashCell == nil or hashCell == 0 ) then
    info("[NOT FOUND]<%s:%s> name(%s) not found, Empty SubRec", MOD, meth,
      tostring( searchName ));
    error( ldte.ERR_NOT_FOUND );
  end

  -- Maybe, eventually, we'll allow a few items to be stored directly
  -- in this directory (to save the SUBREC management for small numbers).
  -- TODO: Add ability to hold small lists -- and read them -- soon.
  local digestString = tostring(hashCell);
  -- Start up a SubRec Context -- for reading subrecs
  local src = createSubrecContext();
  local subRec = openSubrec( src, topRec, digestString );
   	
  if( subrec == nil ) then
    warn("[ERROR]: <%s:%s>: subrec nil or empty: Digest(%s)",  MOD, meth,
      digestString );
    error( ldte.ERR_SUBREC_OPEN );
  end

  local nameList = subRec[LDR_NLIST_BIN];
  if( nameList == nil ) then
    warn("[ERROR]<%s:%s> empty Subrec NameList", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local resultObject = nil;
  local position = searchList( ldtCtrl, nameList, searchName );
  local resultFiltered = nil;
  if( position > 0 ) then
    -- ok -- found the name, so let's process the value.
    local valueList = subRec[LDR_VLIST_BIN];
    resultObject = validateValue( valueList[position] );
  end

  if( resultObject == nil ) then
    warn("[WARNING]<%s:%s> Value not found for name(%s)",
      MOD, meth, tostring( searchName ) );
    error( ldte.ERR_NOT_FOUND );
  end
  resultMap[searchName] = resultObject;
  
  -- NOTE: We could close all subrecs here, but it really doesn't matter
  -- for a single search.
  -- ALSO -- resultMap is returned via parameter, so does not need to be
  -- returned here as a function result.

  GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
                   MOD, meth, tostring(resultMap));
end -- function subRecSearch()

 
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Map Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
local function
localLMapSearch(topRec, ldtBinName, searchName, userModule, filter, fargs)
  local meth = "localLMapSearch()";

  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
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
    subRecSearch( topRec, ldtCtrl, searchName, resultMap );
  end

  GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
     MOD, meth, tostring(resultMap));

  return resultMap;
end -- function localLMapSearch()

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
-- Question  -- Reset the record[ldtBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function localLdtDestroy( topRec, ldtBinName )
  local meth = "localLdtDestroy()";

  GP=E and info("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and lso control map from the lso bin list.

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  
  GP=F and info("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), ldtSummaryString( ldtCtrl ));

  if ldtMap[M_StoreState] ~= SS_COMPACT then 
  	-- Get the ESR and delete it.
	  local esrDigest = propMap[PM_EsrDigest];
          local esrDigestString = tostring(esrDigest);
	  local esrRec = aerospike:open_subrec( topRec, esrDigestString );
	  GP=F and info("[STATUS]<%s:%s> About to Call Aerospike REMOVE", MOD, meth );
	  rc = aerospike:remove_subrec( esrRec );
	  if( rc == nil or rc == 0 ) then
   	    GP=F and info("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
  	  else
    	    warn("[ESR DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, ldtBinName);
    	    error( ldte.ERR_SUBREC_DELETE );
          end
  end 

  -- Mark the enitre control-info structure nil 
  topRec[ldtBinName] = nil;

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.

  local recPropMap = topRec[REC_LDT_CTRL_BIN];

  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid, Contents %s",
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
end -- localLdtDestroy()

-- ==========================================================================
-- ==========================================================================
local function localLMapWalkThru(resultList,topRec,ldtBinName,filter,fargs)
  
  local meth = "localLMapWalkThru()";
  rc = 0; -- start out OK.
  GP=E and info("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );
                 
  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  if ldtMap[M_StoreState] == SS_COMPACT then 
    -- Find the appropriate bin for the Search value
    GP=F and info(" !!!!!! Compact Mode LMAP Search !!!!!");
    -- local binList = ldtMap[M_CompactList];
    list.append( resultList,
      " =========== LMAP WALK-THRU COMPACT MODE \n ================" );
	  
    if ldtMap[M_KeyType] == KT_ATOMIC then
      rc = simpleDumpListAll(topRec, resultList, ldtCtrl, ldtBinName, filter, fargs) 
    else
      rc = complexDumpListAll(topRec, resultList, ldtCtrl, ldtBinName, filter, fargs)
    end
	
    GP=E and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
    -- HACK : TODO : Fix this number to list conversion  
    local digestlist = ldtMap[M_DigestList];
    local src = createSubrecContext();
  
    -- for each digest in the digest-list, open that subrec, send it to our 
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
        rc = ldrSearchList(topRec, ldtBinName, ldrlist, IndexLdrChunk, 0, entryList, filter, fargs );
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
    -- Close ALL of the subrecs that might have been opened
    rc = closeAllSubrecs( src );
  end -- end of else 

  return resultList;
end -- end of localLMapWalkThru

-- =========================================================================
-- localLMapInsertAll()
-- =========================================================================
local function localLMapInsertAll( topRec, ldtBinName, nameValMap, createSpec )
  local meth = "localLMapInsertAll()";
  for name, value in map.pairs( nameValMap ) do
    GP=F and info("[DEBUG]<%s:%s> Processing Arg: Name(%s) Val(%s) TYPE : %s",
        MOD, meth, tostring( name ), tostring( value ), type(value));
    rc = localLMapInsert( topRec, ldtBinName, name, value, createSpec )
    GP=F and info("[DEBUG]<%s:%s> lmap insertion for %s %s RC(%d)",
      MOD, meth, tostring(name), tostring(value), rc );
    return rc; 
  end 
end

-- ========================================================================
-- OLD EXTERNAL FUNCTIONS
-- ======================================================================

-- ======================================================================
-- lmap_search() -- with and without filter
-- ======================================================================
function lmap_search( topRec, ldtBinName, searchName )
  GP=F and info("\n\n >>>>>>>>> API[ lmap_search ] <<<<<<<<<< \n");
  resultMap = map();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue

  validateRecBinAndMap( topRec, ldtBinName, true );
    return localLMapSearch(topRec,ldtBinName,searchName,nil,nil,nil)
end -- lmap_search()

-- ======================================================================

-- ======================================================================
function
lmap_search_then_filter(topRec,ldtBinName,searchName,userModule,filter,fargs )
  GP=F and info("\n\n >>>>>>> API[ lmap_search_then_filter ] <<<<<<<< \n");
  resultMap = map();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue

  validateRecBinAndMap( topRec, ldtBinName, true );
  return localLMapSearch(topRec,ldtBinName,searchName,userModule,filter,fargs);
end -- lmap_search_then_filter()

-- =======================================================================
-- lmap_scan -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type.
-- NOTE: After a bit of thought -- we don't need a separate internal
-- scan function.  Search with a nil searchKey works just fine (I think).
-- =======================================================================
function lmap_scan( topRec, ldtBinName )
  local meth = "lmap_scan()";
  GP=E and info("[ENTER]<%s:%s> LLIST BIN(%s)",
    MOD, meth, tostring(ldtBinName) );

  validateRecBinAndMap( topRec, ldtBinName, true );
  resultMap = map();
  GP=F and info("\n\n  >>>>>>>> API[ SCAN ] <<<<<<<<<<<<<<<<<< \n");

  return localLMapSearchAll(topRec, ldtBinName, resultMap, nil, nil, nil);
end -- end llist_scan()

-- ========================================================================
-- lmap_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function lmap_size( topRec, ldtBinName )
  local meth = "lmap_size()";

  GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  GP=F and info("\n\n >>>>>>>>> API[ LMAP SIZE ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local itemCount = propMap[PM_ItemCount];

  GP=E and info("[EXIT]: <%s:%s> : SIZE(%d)", MOD, meth, itemCount );
  GP=F and info(" !!!!!! lmap_size: Search Key-Type: %s !!!!!",
    tostring(ldtMap[M_KeyType]));

  return itemCount;
end -- function lmap_size()


-- ========================================================================
-- lmap_config() -- return the config settings
-- ========================================================================
function lmap_config( topRec, ldtBinName )
  local meth = "lmap_config()";

  GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  GP=F and info("\n\n >>>>>>>>> API[ LMAP CONFIG ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local config = ldtSummaryString(ldtCtrl); 

  GP=E and info("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config) );
  
  return config;
end -- function lmap_config()

-- ======================================================================
-- lmap_delete() -- with and without filter
-- Return resultMap
-- (*) If successful: return deleted items (map.size( resultMap ) > 0)
-- (*) If error: resultMap will be an empty list.
-- ======================================================================
function lmap_delete( topRec, ldtBinName, searchName )
  return localLMapDelete(topRec, ldtBinName, searchName, nil, nil, nil);
end -- lmap_delete()

function lmap_delete_then_filter( topRec, ldtBinName, searchName,
                                  filter, fargs )
  return localLMapDelete( topRec, ldtBinName, searchName, nil, filter, fargs);
end -- lmap_delete_then_filter()

-- ======================================================================
-- lmap_insert() -- with and without create
-- ======================================================================
function lmap_insert( topRec, ldtBinName, newName, newValue )
  warn("[NONONO]<%s:%s> DO NOT CALL!!!", MOD, meth );
  return localLMapInsert( topRec, ldtBinName, newName, newValue, nil );
end -- lmap_insert()

function lmap_create_and_insert( topRec, ldtBinName, newName, newValue, createSpec )
  warn("[NONONO]<%s:%s> DO NOT CALL!!!", MOD, meth );
  return localLMapInsert( topRec, ldtBinName, newName, newValue, createSpec )
end -- lmap_create_and_insert()

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
      B = true;
      E = true;
    elseif( setting == 0 ) then
      info("[DEBUG SET]<%s:%s> Turn Debug OFF", MOD, meth );
      F = false;
      B = false;
      E = false;
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
-- LMAP Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- (*) put( topRec, ldtBinName, newName, newValue, userModule) 
-- (*) put_all( topRec, ldtBinName, nameValueMap, userModule)
-- (*) get( topRec, ldtBinName, searchName )
-- (*) scan( topRec, ldtBinName )
-- (*) filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) remove( topRec, ldtBinName, searchName )
-- (*) destroy( topRec, ldtBinName )
-- (*) size( topRec, ldtBinName )
-- (*) get_config( topRec, ldtBinName )
-- (*) set_capacity( topRec, ldtBinName, new_capacity)
-- (*) get_capacity( topRec, ldtBinName )
-- ======================================================================
-- The following functions are deprecated:
-- (*) create( topRec, ldtBinName, createSpec )
--
-- The following functions are for development use:
-- dump()
-- debug()
--
-- ======================================================================
-- || create ||
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
function create( topRec, ldtBinName, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP CREATE ] <<<<<<<<<< \n");
  return localLMapCreate( topRec, ldtBinName, createSpec );
end -- create()

-- ======================================================================
-- put() -- Insert a Name/Value pair into the LMAP
-- put_all() -- Insert multiple name/value pairs into the LMAP
-- ======================================================================
function put( topRec, ldtBinName, newName, newValue, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP PUT ] <<<<<<<<<< \n");
  return localLMapInsert( topRec, ldtBinName, newName, newValue, createSpec )
end -- put()

function put_all( topRec, ldtBinName, NameValMap, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP PUT ALL] <<<<<<<<<< \n");
  return localLMapInsertAll( topRec, ldtBinName, NameValMap, createSpec )
end -- put_all()

-- ========================================================================
-- get() -- Return a map containing the searched-for name/value pair.
-- scan() -- Return a map containing ALL name/value pairs.
-- ========================================================================
-- ========================================================================
function get( topRec, ldtBinName, searchName )
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP GET ] <<<<<<<<<< \n");
  return localLMapSearch(topRec, ldtBinName, searchName, nil, nil)
end -- get()

function scan( topRec, ldtBinName )
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP SCAN ] <<<<<<<<<< \n");
  return localLMapSearchAll(topRec, ldtBinName, nil, nil)
end -- scan()

-- ========================================================================
-- filter() -- Return a map containing all Name/Value pairs that passed
--             thru the supplied filter( fargs ).
-- ========================================================================
function filter( topRec, ldtBinName, userModule, filter, fargs )
  GP=B and info("\n\n  >>>>>>>> API[ FILTER ] <<<<<<<<<<<<<<<<<< \n");
  return localLMapSearchAll(topRec, ldtBinName, userModule, filter, fargs);
end -- filter()

-- ========================================================================
-- remove() -- Remove the name/value pair matching <searchName>
-- ========================================================================
function remove( topRec, ldtBinName, searchName )
  GP=B and info("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");
  return localLMapDelete(topRec, ldtBinName, searchName, nil, nil, nil )
end -- remove()

-- ========================================================================
-- destroy() - Entirely obliterate the LDT (record bin value and all)
-- ========================================================================
function destroy( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>> API[ DESTROY ] <<<<<<<<<< \n");
  return localLdtDestroy( topRec, ldtBinName );
end -- destroy()

-- ========================================================================
-- size() -- return the number of elements (item count) in the set.
-- ========================================================================
function size( topRec, ldtBinName )
  local meth = "size()";
  GP=B and info("\n\n >>>>>>>>> API[ LMAP SIZE ] <<<<<<<<<< \n");

  GP=E and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[1]; 
  local ldtMap = ldtCtrl[2]; 
  local itemCount = propMap[PM_ItemCount];

  GP=E and trace("[EXIT]: <%s:%s> : SIZE(%d)", MOD, meth, itemCount );
  return itemCount;
end -- size()

-- ========================================================================
-- get_config() -- return the config settings
-- ========================================================================
function get_config( topRec, ldtBinName )
  local meth = "get_config()";
  GP=B and info("\n\n >>>>>>>>> API[ LMAP CONFIG ] <<<<<<<<<< \n");

  GP=E and info("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local config = ldtSummary(ldtCtrl); 

  GP=E and info("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config) );
  return config;
end -- get_config();

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
  return localGetCapacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  return localSetCapacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
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
function dump( topRec, ldtBinName )
  local meth = "dump()";
  GP=E and info("[ENTER]<%s:%s> ", MOD, meth);

  GP=E and info("[ENTER]<%s:%s> LLIST BIN(%s)",
    MOD, meth, tostring(ldtBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  resultList = list();
  GP=F and info("\n\n  >>>>>>>> API[ DUMP ] <<<<<<<<<<<<<<<<<< \n");

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
end -- dump();

-- ========================================================================
-- debug() -- turn on/off our debug settings
-- ========================================================================
function debug( topRec, setting )
  return localDebug( topRec, setting );
end

-- <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
--   _     ___  ___  ___  ______ 
--  | |    |  \/  | / _ \ | ___ \
--  | |    | .  . |/ /_\ \| |_/ /
--  | |    | |\/| ||  _  ||  __/ 
--  | |____| |  | || | | || |    
--  \_____/\_|  |_/\_| |_/\_|    
--                               
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
