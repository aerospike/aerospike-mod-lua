-- Large Stack Object (LSO or LSTACK) Operations
-- lmap.lua:  July 12, 2013
--
-- Module Marker: Keep this in sync with the stated version
local MOD="lmap_2013_07_12.0"; -- the module name used for tracing

-- This variable holds the version of the code (Major.Minor).
-- We'll check this for Major design changes -- and try to maintain some
-- amount of inter-version compatibility.
local G_LDT_VERSION = 1.0;

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true; -- Leave this ALWAYS true (but value seems not to matter)
local F=true; -- Set F (flag) to true to turn ON global print

-- ======================================================================
-- !!!!! Please refer to lmap_design.lua for architecture and design notes.!!!! 
-- ======================================================================
-- Get addressability to the Function Table: Used for compress and filter
-- set up our "outside" links
local  CRC32 = require('CRC32');
local functionTable = require('UdfFunctionTable');

-- This flavor of LDT
local LDT_LMAP   = "LMAP";

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
local DEFAULT_THRESHHOLD = 100;
-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
local MAGIC="MAGIC";     -- the magic value for Testing LSO integrity

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

-- AS LMAP Bin Names
local LMAP_CONTROL_BIN       = "LMapCtrlBin";

-- ++===============++
-- || Package Names ||
-- ++===============++
-- Specific Customer Names (to be moved out of the System Table)
local PackageStumbleUpon     = "StumbleUpon";

-- Standard, Test and Debug Packages
local PackageStandardList    = "StandardList";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
local PackageDebugModeList   = "DebugModeList";
local PackageDebugModeBinary = "DebugModeBinary";
local PackageDebugModeNumber = "DebugModeNumber";

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

-- LDT TYPES 
local LDT_TYPE_LMAP = "LMAP";

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

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
local PM_LdtType               = 'T'; -- (Top): Type: stack, set, map, list
local PM_BinName               = 'B'; -- (Top): LDT Bin Name
local PM_Magic                 = 'Z'; -- (All): Special Sauce
local PM_EsrDigest             = 'E'; -- (All): Digest of ESR
local PM_RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
local PM_LogInfo               = 'L'; -- (All): Log Info (currently unused)
local PM_ParentDigest          = 'P'; -- (Subrec): Digest of TopRec
local PM_SelfDigest            = 'D'; -- (Subrec): Digest of THIS Record
local PM_CreateTime			   = 'C';

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSO Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lset & lmap 
local M_StoreMode              = 'M';
local M_Transform              = 't';
local M_UnTransform            = 'u';
local M_KeyCompare             = 'k'; 
local M_LdrEntryCountMax       = 'e';
local M_LdrByteEntrySize       = 's';
local M_LdrByteCountMax        = 'b';
local M_StoreState             = 'S'; 
local M_BinaryStoreSize        = 'B'; 
local M_KeyType                = 'K'; 
local M_TotalCount             = 'N'; 
local M_Modulo 				   = 'O';
local M_ThreshHold             = 'H';

-- Fields specific to lmap in the standard mode only. In standard mode lmap 
-- does not resemble lset, it looks like a fixed-size warm-list from lstack
-- with a digest list pointing to LDR's. 

local M_DigestList             = 'W';
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
-- S:M_StoreLimit             s:M_LdrByteEntrySize
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
local LDR_CTRL_BIN = "LdrControlBin";  
local LDR_LIST_BIN = "LdrListBin";  
local LDR_BNRY_BIN = "LdrBinaryBin";

-- All LDT subrecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin.
local SUBREC_PROP_BIN="SR_PROP_BIN";
--
-- Bin Flag Types
local BF_LDT_BIN     = 1; -- Main LDT Bin
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

-- Bin Flag Types
local BF_LDT_BIN    = 'L';  -- Main LDT Bin
local BF_LDT_HIDDEN = 'H';  -- Set the Hidden Flag on this bin
-- Package Names for "pre-packaged" settings:
local PackageStandardList=   "StandardList";
local PackageTestModeList=   "TestModeList";
local PackageTestModeBinary= "TestModeBinary";
-- Specific production use: 
-- (*) A List Value (a 5 part tuple)
-- (*) Special, packed (compressed) Binary storage
local PackageProdListValBinStore=    "ProdListValBinStore";
local PackageDebugModeList=  "DebugModeList";
local PackageDebugModeBinary="DebugModeBinary";
-- ------------------------------------------------------------------------
-- ======================================================================
-- local function lmapSummary( lmapList ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lmapList 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function lmapSummary( lmapList )
  if ( lmapList == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    return "EMPTY LDT BIN VALUE";
  end

  local propMap = lmapList[1];
  local lmapCtrlInfo  = lmapList[2];
  
  if( propMap[PM_Magic] ~= MAGIC ) then
    return "BROKEN MAP--No Magic";
  end;

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();

  -- Properties
  -- Fields common for all LDT's
  resultMap.SUMMARY              = "LMAP Summary";
  resultMap.PropItemCount        = propMap[PM_ItemCount];
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
  resultMap.StoreMode            = lmapCtrlInfo[M_StoreMode];
  resultMap.Transform            = lmapCtrlInfo[M_Transform];
  resultMap.UnTransform          = lmapCtrlInfo[M_UnTransform];
  resultMap.KeyCompare           = lmapCtrlInfo[M_KeyCompare];
  resultMap.BinaryStoreSize      = lmapCtrlInfo[M_BinaryStoreSize];
  resultMap.KeyType              = lmapCtrlInfo[M_KeyType];
  resultMap.TotalCount			 = lmapCtrlInfo[M_TotalCount];		
  resultMap.Modulo 				 = lmapCtrlInfo[M_Modulo];
  resultMap.ThreshHold			 = lmapCtrlInfo[M_ThreshHold];
  
  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = lmapCtrlInfo[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = lmapCtrlInfo[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = lmapCtrlInfo[M_LdrByteCountMax];

  -- Digest List Settings: List of Digests of LMAP Data Records
  -- specific to LMAP in STANDARD_MODE ONLY 
  
  resultMap.DigestList        = lmapCtrlInfo[M_DigestList];
  resultMap.TopFull 		  = lmapCtrlInfo[M_TopFull];
  resultMap.ListDigestCount   = lmapCtrlInfo[M_ListDigestCount];
  resultMap.ListMax           = lmapCtrlInfo[M_ListMax];
  resultMap.TopChunkByteCount = lmapCtrlInfo[M_TopChunkByteCount];
  resultMap.TopChunkEntryCount= lmapCtrlInfo[M_TopChunkEntryCount];

  return resultMap;
end -- lsoSummary()

-- ======================================================================
-- Make it easier to use lsoSummary(): Have a String version.
-- ======================================================================
local function lmapSummaryString( lmapList )
    return tostring( lmapSummary( lmapList ) );
end

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two main Record Types used in the LSO Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSO bin
-- (*) lmapBinName: the LSO Data Record that holds user Data
-- (*) compact_mode_flag : decides LMAP storage mode : SS_COMPACT or SS_REGULAR
--
-- <+> Naming Conventions:
--   + All Field names (e.g. lmapCtrlInfo.StoreMode) begin with Upper Case
--   + All variable names (e.g. lmapCtrlInfo.StoreMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec[LDR_CTRL_BIN]);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ======================================================================
-- initializeLsoMap:
-- ======================================================================
-- Set up the LMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LMAP BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LMAP
-- behavior.  Thus this function represents the "type" LMAP -- all
-- LMAP control fields are defined here.
-- The LMap is obtained using the user's LMap Bin Name:
-- ======================================================================

local function initializeLMap( topRec, lmapBinName, compact_mode_flag )
  local meth = "initializeLMap()";
  
  -- Create 2 maps : The generic property map 
  -- and lmap specific property map. Create one
  -- list : the actual LDR list for lmap. 
  -- Note: All Field Names start with UPPER CASE.
  local lmapCtrlInfo = map();
  local propMap = map(); 
  local lmapList = list(); 
  
  
  GP=F and info("[ENTER]: <%s:%s>:: Compact-Mode LMapBinName(%s)",
  MOD, meth, tostring(lmapBinName));
  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LMAP; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = lmapBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]  = nil; -- not set yet.
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
--  propMap[PM_CreateTime] = aerospike:get_current_time();
  
-- Specific LMAP Parms: Held in LMap
  lmapCtrlInfo[M_StoreMode]  = SM_LIST; -- SM_LIST or SM_BINARY:

  -- LMAP Data Record Chunk Settings: Passed into "Chunk Create"
  lmapCtrlInfo[M_LdrEntryCountMax]   = 100;  -- Max # of Data Chunk items (List Mode)
  lmapCtrlInfo[M_LdrByteEntrySize]   =  0;  -- Byte size of a fixed size Byte Entry
  lmapCtrlInfo[M_LdrByteCountMax]    =   0; -- Max # of Data Chunk Bytes (binary mode)
  lmapCtrlInfo[M_Transform]          = nil; -- applies only to complex lmap
  lmapCtrlInfo[M_UnTransform]        = nil; -- applies only to complex lmap
  lmapCtrlInfo[M_KeyCompare]         = nil; -- applies only to complex lmap
  lmapCtrlInfo[M_StoreState]         = SS_COMPACT; -- SM_LIST or SM_BINARY:
  lmapCtrlInfo[M_BinaryStoreSize]    = nil; 
  lmapCtrlInfo[M_KeyType]            = KT_ATOMIC; -- assume "atomic" values for now.
  lmapCtrlInfo[M_TotalCount]         = 0; -- Count of both valid and deleted elements
  lmapCtrlInfo[M_Modulo]             = DEFAULT_DISTRIB; -- Currently this is 31
  lmapCtrlInfo[M_ThreshHold]         = 101; -- Rehash after this many have been inserted
	  
 if compact_mode_flag == false then
       -- ======  Begin : Regular mode settings ==================================
  
   	  -- we are now in rehashSettoLmap(), we need to change lmapCtrlIndo params  
   	  -- all the other params must already be set by default. 
   	  GP=F and info("[ENTER]: <%s:%s>:: Regular-Mode LMapBinName(%s)",
      MOD, meth, tostring(lmapBinName));
  	  lmapCtrlInfo[M_StoreState]  = SS_REGULAR; -- SM_LIST or SM_BINARY:
  	  
      -- Digest List Settings: List of Digests of LMAP Data Records
      propMap[PM_ParentDigest]          = 'P'; -- (Subrec): Digest of TopRec
      propMap[PM_SelfDigest]            = 'D'; -- (Subrec): Digest of THIS Record
      lmapCtrlInfo[M_DigestList]        = list(); -- the list of digests for LDRs
  
      -- true when the list of entries pointed to by a digest is full (for next write)
      -- When this flag is set, we'll do a new chunk-create + new digest entry in 
      -- digest-list vs simply an entry-add to the list
      lmapCtrlInfo[M_TopFull] = false; 
  
      -- How many LDR chunks (entry lists) exist in this lmap bin 
      lmapCtrlInfo[M_ListDigestCount]   = 0; -- Number of Warm Data Record Chunks
      
      -- This field is technically used to determine if warm-list has any more room 
      -- of if we want to age and transfer some items to cold-list to make room. 
      -- Since there is no overflow, this might not be needed really ? or we can 
      -- reuse it to determine something else -- Check with Toby
      
      lmapCtrlInfo[M_ListMax]           = 100; -- Max Number of Data Record Chunks
      lmapCtrlInfo[M_TopChunkEntryCount]= 0; -- Count of entries in top chunks
      lmapCtrlInfo[M_TopChunkByteCount] = 0; -- Count of bytes used in top Chunk
 
  -- ====== End : Standard mode settings ==================================
  end
  
  -- Put our new maps in a list, in the record, then store the record.
  list.append( lmapList, propMap );
  list.append( lmapList, lmapCtrlInfo );
  
  -- Once this list of 2 maps is created, we need to assign it to topRec
  topRec[LMAP_CONTROL_BIN]            = lmapList;
  

  GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after Init(%s)",
      MOD, meth , lmapSummaryString(lmapList));

  GP=F and info("[EXIT]:<%s:%s>:", MOD, meth );
  return lmapList;
  
end -- initializeLMap

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateLmapBinName( binName )
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
end -- validateLmapBinName

-- ======================================================================
-- validateLmapParams():
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- for lmap_create, mustExist is false
-- This also gets called for any other lmap-param like search, insert, delete etc 
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateLmapParams( topRec, lmapBinName, mustExist )
  local meth = "validateLmapParams()";
  GP=F and info("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( lmapBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  validateLmapBinName( lmapBinName );

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
      error('Base Record Does NOT exist');
    end

    -- Control Bin Must Exist
    if( topRec[LMAP_CONTROL_BIN] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LMAP BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(lmapBinName) );
      error('LMAP BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local lMapList = topRec[LMAP_CONTROL_BIN] ; -- The main lsoMap structure
    -- Extract the property map and lso control map from the lso bin list.
    local propMap = lMapList[1];
    local lsoMap  = lMapList[2];

    if propMap[PM_Magic] ~= MAGIC then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LMAP BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( lmapBinName ) );
      error('LMAP BIN Is Corrupted (No Magic::1)');
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[LMAP_CONTROL_BIN] ~= nil then
      local lsoList = topRec[LMAP_CONTROL_BIN]; -- The main lsoMap structure
      -- Extract the property map and lso control map from the lso bin list.
      local propMap = lsoList[1];
      local lsoMap  = lsoList[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LMAP BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( lmapBinName ) );
        error('LMAP BIN Is Corrupted (No Magic::2)');
      end
    end -- if worth checking
  end -- else for must exist

end -- validateLmapParams()

-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
local function packageStandardList( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- Atomic Keys
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo]= DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- Atomic Keys
--  lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = "compressTest4";
  lmapCtrlInfo[M_UnTransform] = "unCompressTest4";
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted

end -- packageTestModeBinary( lmapCtrlInfo )

-- ======================================================================
-- Package = "StumbleUpon"
-- StumbleUpon uses a compacted representation.
-- NOTE: This will eventually move to the UDF Function Table, or to a
-- separate Configuration file.  For the moment it is included here for
-- convenience. 
-- ======================================================================
local function packageStumbleUpon( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = "compress4ByteInteger";
  lmapCtrlInfo[M_UnTransform] = "unCompress4ByteInteger";
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_BINARY; -- Use a Byte Array
  lmapCtrlInfo[M_BinaryStoreSize] = 4; -- Storing a single 4 byte integer
  lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- Atomic Keys (a number)
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 100; -- Rehash after this many have been inserted
  
end -- packageStumbleUpon( lmapCtrlInfo )

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LMAP with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- Atomic Keys
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( lmapCtrlInfo )
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = "compressTest4";
  lmapCtrlInfo[M_UnTransform] = "unCompressTest4";
  lmapCtrlInfo[M_KeyCompare] = "debugListCompareEqual"; -- "Simple" list comp
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = 16; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- special function for list compare.
 -- lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted

end -- packageDebugModeBinary( lmapCtrlInfo )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( lmapCtrlInfo )
  local meth = "packageDebugModeNumber()";
  GP=F and trace("[ENTER]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring( lmapCtrlInfo ));
  
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_KeyCompare] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode ]= SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted

  GP=F and trace("[EXIT]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring( lmapCtrlInfo ));
end -- packageDebugModeNumber( lmapCtrlInfo )

-- ======================================================================
-- adjustLMapCtrlInfo:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the lmapCtrlInfo.
-- Parms:
-- (*) lmapCtrlInfo: the main Lmap Control Bin value
-- (*) argListMap: Map of LMAP Settings 
-- ======================================================================
local function adjustLMapCtrlInfo( lmapCtrlInfo, argListMap )
  local meth = "adjustLMapCtrlInfo()";
  GP=F and info("[ENTER]: <%s:%s>:: LMapCtrlInfo(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(lmapCtrlInfo), tostring( argListMap ));

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
          packageStandardList( lmapCtrlInfo );
      elseif value == PackageTestModeList then
          packageTestModeList( lmapCtrlInfo );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( lmapCtrlInfo );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( lmapCtrlInfo );
      elseif value == PackageStumbleUpon then
          packageStumbleUpon( lmapCtrlInfo );
      elseif value == PackageDebugModeList then
          packageDebugModeList( lmapCtrlInfo );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( lmapCtrlInfo );
      elseif value == PackageDebugModeNumber then
          packageDebugModeNumber( lmapCtrlInfo );
      end
    elseif name == "KeyType" and type( value ) == "string" then
      -- Use only valid values (default to ATOMIC if not specifically complex)
      -- Allow both upper and lower case versions of "complex".
      if value == KT_COMPLEX or value == "complex" then
        lmapCtrlInfo[M_KeyType] = KT_COMPLEX;
      else
        lmapCtrlInfo[M_KeyType] = KT_ATOMIC; -- this is the default.
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        lmapCtrlInfo[M_StoreMode] = value;
      end
    elseif name == "Modulo"  and type( value ) == "number" then
      -- Verify it's a valid value
      if value > 0 and value < DEFAULT_DISTRIB then
        lmapCtrlInfo[M_Modulo] = value;
      end
    end
  end -- for each argument

  GP=F and info("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(lmapCtrlInfo));
      
  return lmapCtrlInfo
end -- adjustLMapCtrlInfo

-- ======================================================================
-- setupNewLmapBin: Initialize a new bin -- (the thing that holds a list
-- of user values).
-- Parms:
-- (*) topRec
-- (*) Bin Name -- Lmap is all about user-defined bin-names
-- Return: New Bin Name
-- ======================================================================
local function setupNewLmapBin( topRec, binName )
  local meth = "setupNewLmapBin()";
  GP=F and info("[ENTER]: <%s:%s> BinName(%s) ", MOD, meth, tostring(binName) );

  -- create the first LMap type LDT bin
  topRec[binName] = list(); -- Create a new list for this new bin

  -- TODO : Code for standard mode
  return 0;
end -- setupNewLmapBin

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LMAP Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || lmap_create ||
-- ======================================================================
-- Create/Initialize a Map structure in a bin, using a single LMAP
-- bin, using User's name, but Aerospike TYPE (AS_LMAP)
--
-- For this version (Stoneman), we will be using a SINGLE MAP object,
-- which contains lots of metadata, plus one list:
-- (*) Namespace Name (just one Namespace -- for now)
-- (*) Set Name
-- (*) Chunk Size (same for both namespaces)
-- (*) Item Count (will NOT be tracked in Stoneman)
-- (*) The List of Digest Chunks of data (each Chunk is a list)
-- (*) Storage Mode (Compact or Regular) (0 for compact, 1 for regular)
-- (*) Compact Item List
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
-- (2) lMapBinName: The name of the LMAP Bin
-- (3) createSpec: The map (not list) of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- TODO : Code for standard mode

--
-- ========================================================================
function lmap_create( topRec, lmapBinName, createSpec )
  local meth = "lmap_create()";
  
  GP=F and info("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(lmapBinName), tostring(createSpec) );
                 
  if createSpec == nil then
    GP=F and info("[ENTER1]: <%s:%s> lmapBinName(%s) NULL createSpec",
      MOD, meth, tostring(lmapBinName));
  else
    GP=F and info("[ENTER2]: <%s:%s> lmapBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( lmapBinName), tostring( createSpec ));
  end

  -- Some simple protection of faulty records or bad bin names
  validateLmapParams( topRec, lmapBinName, false );

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LMAP field to an
  -- existing record.
  if( topRec[LMAP_CONTROL_BIN] ~= nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> LMAP CONTROL BIN Already Exists",
                   MOD, meth );
    error('LMAP CONTROL BIN already exists');
  end
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.
  
  -- This will throw and error and jump out of Lua if binName is bad.
  -- NOTE: Not needed until we switch to using a SINGLE BIN for LMAP.
  -- validateBinName( lsetBinName );

  GP=F and info("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
  local compact_mode_flag = true; 
  local lMapList = initializeLMap( topRec, lmapBinName, compact_mode_flag );
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  
  -- Set the type of this record to LDT (it might already be set)
  record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in some settings that override our defaults
  -- (createSpec) then apply them now.
  if createSpec ~= nil then 
    adjustLMapCtrlInfo( lmapCtrlInfo, createSpec );
    -- Changes to the map need to be re-appended to topRec  
    local NewLmapList = list();
    list.append( NewLmapList, propMap );
    list.append( NewLmapList, lmapCtrlInfo );
    topRec[LMAP_CONTROL_BIN] = NewLmapList;
    
    GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after adjustLMapCtrlInfo(%s)",
      MOD, meth , lmapSummaryString(NewLmapList));
  end

  -- initializeLMap sets lMapCtrlInfo.StoreState to SS_COMPACT when 
  -- compact_mode_flag = true
  -- At this point there is only one bin.
  -- This one will assign the actual record-list to topRec[binName]
  setupNewLmapBin( topRec, lmapBinName );

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and info("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- lmap_create()

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
  GP=F and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
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
local function computeSetBin( newValue, lmapCtrlInfo )
  local meth = "computeSetBin()";
  GP=F and info("[ENTER]: <%s:%s> val(%s) Map(%s) ",
                 MOD, meth, tostring(newValue), tostring(lmapCtrlInfo) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local binNumber  = 0;
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then
    -- In the case of LMAP, we dont need to worry about this
    -- because we never call this for compact
    return 0
  else
    if type(newValue) == "number" then
      binNumber  = numberHash( newValue, lmapCtrlInfo[M_Modulo] );
    elseif type(newValue) == "string" then
      binNumber  = stringHash( newValue, lmapCtrlInfo[M_Modulo] );
    elseif type(newValue) == "userdata" then
      -- We are assuming that the user has supplied a function for us to
      -- deal with a complex object.  If no function, then error.
      -- Note that the easy case is the keyHashFunction(), which is a
      -- hash on a field called "KEY".

      -- TODO: Fix this
      print("COMPUTE SET BIN::MUST USE EXTRACT FUNCTION HERE!!!");

      print("MUST REGISTER A HASH FUNCTION FOR COMPLEX TYPES!!");

      binNumber  = stringHash( newValue.KEY, lmapCtrlInfo[M_Modulo]);
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type (should be number, string or map)",
           MOD, meth );
      error('ERROR: Incorrect Type for new Large Set value');
    end
  end
  GP=F and info("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) ",
                 MOD, meth, tostring(newValue), binNumber );

  return binNumber;
end -- computeSetBin()

-- ========================================================================
-- lmap_subrec_list() -- Return a list of subrecs
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
function lmap_subrec_list( topRec, lsoBinName )
  local meth = "lmap_subrec_list()";

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

end -- lmap_subrec_list()

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

  resultMap.List = ldrChunkRecord[LDR_LIST_BIN];
  resultMap.ListSize = list.size( resultMap.List );

  return tostring( resultMap );
end -- ldrChunkSummary()

-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- ======================================================================
local function setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";
  GP=F and info("[ENTER]<%s:%s>", MOD, meth );

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if( topRec[REC_LDT_CTRL_BIN] == nil ) then
    GP=F and info("[DEBUG]<%s:%s>Creating Record LDT Map", MOD, meth );
    record.set_type( topRec, RT_LDT );
    recPropMap = map();
    -- vinfo will be a 5 byte value, but it will be easier for us to store
    -- 6 bytes -- and just leave the high order one at zero.
    -- Initialize the VINFO value to all zeros.
    local vinfo = bytes(6);
    bytes.put_int16(vinfo, 1, 0 );
    bytes.put_int16(vinfo, 3, 0 );
    bytes.put_int16(vinfo, 5, 0 );
    recPropMap[RPM_VInfo] = vinfo; 
    recPropMap[RPM_LdtCount] = 1; -- this is the first one.
    recPropMap[RPM_Magic] = MAGIC;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;    
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM_LdtCount];
    recPropMap[RPM_LdtCount] = ldtCount + 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    GP=F and info("[DEBUG]<%s:%s>Record LDT Map Exists: Bump LDT Count(%d)",
      MOD, meth, ldtCount + 1 );
  end

  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  rc = aerospike:update( topRec );

  GP=F and info("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
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
local function createAndInitESR( topRec )
  local meth = "createAndInitESR()";
  GP=F and info("[ENTER]: <%s:%s>", MOD, meth );

  local lMapList = topRec[LMAP_CONTROL_BIN] ;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  
  local rc = 0;
  local esr       = aerospike:create_subrec( topRec );
  local esrDigest = record.digest( esr );
  local topDigest = record.digest( topRec );

  local esrPropMap = map(); 
  
  esrPropMap[PM_Magic]        = MAGIC;
  esrPropMap[PM_RecType]   = RT_ESR;
  esrPropMap[PM_ParentDigest] = topDigest; -- Parent
  esrPropMap[PM_EsrDigest] = esrDigest; -- Self
  esrPropMap[PM_SelfDigest]   = subDigest;
  
  -- Set the record type as "ESR"
  trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esr, RT_ESR );
  trace("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );
  
  esr[SUBREC_PROP_BIN] = esrPropMap;

  GP=F and info("[EXIT]: <%s:%s> Leaving with ESR Digest(%s)",
    MOD, meth, tostring(esrDigest));

  rc = aerospike:update_subrec( esr );
  if( rc == nil or rc == 0 ) then
      aerospike:close_subrec( esr );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error("[ESR CREATE] Error Creating System Subrecord");
  end

  -- update global attributes. 
  propMap[PM_EsrDigest] = esrDigest; 
  
  local NewlMapList = list();
  list.append( NewlMapList, propMap );
  list.append( NewlMapList, lmapCtrlInfo );
  topRec[LMAP_CONTROL_BIN] = NewlMapList;
  
  -- If the topRec already has an REC_LDT_CTRL_BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  setLdtRecordType( topRec );

  return esrDigest;

end -- createAndInitESR()

-- ======================================================================
-- initializeSubrecLdrMap()
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
-- (*) ldrRec[LDR_LIST_BIN]: The Data Entry List (when in list mode)
-- (*) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
--
-- When we call this method, we have just created a LDT SubRecord.  Thus,
-- we must check to see if that is the FIRST one, and if so, we must also
-- create the Existence Sub-Record for this LDT.
-- ======================================================================

local function initializeSubrecLdrMap( topRec, newLdrChunkRecord, ldrPropMap, ldrMap, lMapList)
  local meth = "initializeSubrecLdrMap()";
  GP=F and info("[ENTER]: <%s:%s>", MOD, meth );

  local propMap = lMapList[1];
  local lmapCtrlInfo = lMapList[2];
  local binName    = propMap[PM_BinName];

  -- topRec's digest is the parent digest for this subrec 
  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  -- Subrec's (its own) digest is the selfDigest :)
  ldrPropMap[PM_SelfDigest]   = record.digest( newLdrChunkRecord ); 

  --  Use Top level LMAP entry for mode and max values
  ldrMap[LDR_ByteEntrySize]   = lmapCtrlInfo[M_LdrByteEntrySize];
  ldrMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries

   GP=F and info("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
   MOD, meth, tostring(ldrPropMap) );
   
  -- If this is the first LDR, then it's time to create an ESR for this
  -- LDT. There is one ESR created per LMAP bin, not per LDR chunk creation.
  if( propMap[PM_EsrDigest] == nil or ldrPropMap[PM_EsrDigest] == 0 ) then
    GP=F and info(" !!!!!!!!!!! First ESR creation for LDT bin !!!!!!!!!!!!!!!");
    ldrPropMap[PM_EsrDigest] = createAndInitESR( topRec );
  end

end -- initializeSubrecLdrMap()

-- ======================================================================
-- lmapLdrListChunkCreate( topRec, lMapList )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
-- In this function, we create a LDR subrec and init two structures: 
-- a. The property-map for the new LDR subrec chunk
-- b. The ctrl-map for the new LDR subrec chunk record
-- a & b are done in initializeSubrecLdrMap()
-- Once that is done in the called-function, we then make a call to create 
-- an ESR and init that struct as well in createAndInitESR(). 
-- From the above function, we call setLdtRecordType() to do some 
-- byte-level magic on the ESR property-map structure. 

local function   lmapLdrListChunkCreate( topRec, lMapList )
  local meth = "lmapLdrListChunkCreate()";
  GP=F and info("[ENTER]: <%s:%s> ", MOD, meth );
  
  -- TODO : we need to add a check to even see if we can accomodate any more 
  
  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newLdrChunkRecord = aerospike:create_subrec( topRec );
  
  if newLdrChunkRecord == nil then 
    warn("[ERROR]<%s:%s>Problems Creating Subrec New-entry(%s)",MOD,meth,tostring(newLdrChunkRecord));
    error("[SUBREC-CREATE] Error Creating System Subrecord");
    return newLdrChunkRecord;
  end
  
  -- local topLdrChunk = aerospike:open_subrec( topRec, stringDigest );
  local ldrPropMap = map();
  local ldrMap = map();
  local newChunkDigest = record.digest( newLdrChunkRecord );
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName    = propMap[PM_BinName];

  -- Each subrec that gets created, needs to have its properties initialized. 
  -- Also the ESR structure needs to get created, if needed
  -- Plus the REC_LDT_CTRL_BIN of topRec needs to be updated. 
  -- This function takes care of doing all of that. 
  initializeSubrecLdrMap( topRec, newLdrChunkRecord, ldrPropMap, ldrMap, lMapList );

  -- Assign Prop, Control info and List info to the LDR bins
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;
  newLdrChunkRecord[LDR_CTRL_BIN] = ldrMap;
  newLdrChunkRecord[LDR_LIST_BIN] = list();

  GP=F and info("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrPropMap) );
  
  rc = aerospike:update_subrec( newLdrChunkRecord );
  if( rc == nil or rc == 0 ) then
      aerospike:close_subrec( newLdrChunkRecord );
  else
    warn("[ERROR]<%s:%s>Problems Updating newLdrChunkRecord rc(%s)",MOD,meth,tostring(rc));
    error("[newLdrChunkRecord CREATE] Error Creating System Subrecord");
  end

  -- Add our new chunk (the digest) to the WarmDigestList
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
  GP=F and info("[DEBUG]: <%s:%s> Appending NewChunk with digest(%s) to DigestList(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(lmapCtrlInfo[M_DigestList]));

  -- THE LINE TO ADD A NEW DIGEST TO THE LMAP LIST IN REGULAR MODE
  -- This is the only line that separates LMAP from LSTACK. In the case of 
  -- LSTACK, we simply add the new digest to the list of warm-digests.  
  -- In the case of LMAP, we take the digest of the top-most entry in our
  -- ldr-list-bin, obtain the list-entry corresponding to it, hash this 
  -- list-entry over M_Modulo digest-bins and then insert the digest into that
  -- index pointing matching for it hash it over the list of digests with 
  -- Modulo-N and then insert the digest into the 
  -- list.append( lmapCtrlInfo[M_DigestList], newChunkDigest );

  GP=F and info("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LMap(%s): ",
    MOD, meth, tostring(newChunkDigest), tostring(lmapCtrlInfo));
   
  -- Increment the Warm Count
  -- gets inceremented once per LDR entry add. 
  local ChunkCount = lmapCtrlInfo[M_ListDigestCount]; 
  lmapCtrlInfo[M_ListDigestCount] = (ChunkCount + 1);

  -- NOTE: This may not be needed -- we may wish to update the topRec ONLY
  -- after all of the underlying SUB-REC  operations have been done.
  -- Update the top (LSO) record with the newly updated lsoMap;
  -- NewtopRec[ binName ] = lmapCtrlInfo;

  --GP=F and trace("[EXIT]: <%s:%s> Return(%s) ",
  --  MOD, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  lmapLdrListChunkCreate()

-- ======================================================================
-- ldrInsertList( topLdrChunk, lMapList, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values pointed to from the digest-list, 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lMapList: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================

local function ldrInsertList(ldrChunkRec,lMapList,listIndex,insertList )
  local meth = "ldrInsertList()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];

  GP=F and trace("[DEBUG]<%s:%s> LSO MAP(%s)", MOD, meth, tostring(lsoMap));

  -- These 2 get assigned in lmapLdrListChunkCreate() to point to the ctrl-map. 
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  local ldrValueList = ldrChunkRec[LDR_LIST_BIN];
  local chunkIndexStart = list.size( ldrValueList ) + 1;
  local ldrByteArray = ldrChunkRec[LDR_BNRY_BIN]; -- might be nil

  GP=F and info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSlotsAvailable = (lmapCtrlInfo[M_LdrEntryCountMax] - chunkIndexStart) + 1;

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
    lmapCtrlInfo[M_TopFull] = true;
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
  GP=F and info("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
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

  GP=F and info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()

-- ======================================================================
-- ldrInsertBytes( topLdrChunk, lMapList, listIndex,  insertList )
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
-- (*) lMapList: the LMAP control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertBytes( ldrChunkRec, lMapList, listIndex, insertList )
  local meth = "ldrInsertBytes()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = lmapCtrlInfo[M_LdrByteEntrySize];
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
  local maxEntries = math.floor(lmapCtrlInfo[M_LdrByteCountMax] / entrySize );
  local itemSlotsAvailable = maxEntries - entryCount;
  GP=F and
    info("[DEBUG]: <%s:%s>:MaxEntries(%d) SlotsAvail(%d) #Total ToWrite(%d)",
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
    lmapCtrlInfo[M_TopFull] = true; -- Remember to reset on next update.
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
    GP=F and trace("[DEBUG]:<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
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

  GP=F and info("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring( chunkByteArray ));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec[LDR_CTRL_BIN] = ldrMap;
  ldrChunkRec[LDR_BNRY_BIN] = chunkByteArray;

  GP=F and info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()


-- ======================================================================
-- ldrInsert(ldrChunkRec,lMapList,listIndex,insertList )
-- ======================================================================
-- Insert (append) the LIST of values to the digest-list created for LMAP. 
-- !!!!!    This is applicable only in SS_REGULAR mode !!!!!!!!!!!!!!!!!!!
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in SM_LIST mode or
-- SM_BINARY mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lMapList: the LMAP control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsert(ldrChunkRec,lMapList,listIndex,insertList )
  local meth = "ldrInsert()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s), ChunkSummary(%s)",
    MOD, meth, listIndex, tostring( insertList ),ldrChunkSummary(ldrChunkRec));
    
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];

  if lmapCtrlInfo[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,lMapList,listIndex,insertList );
  else
    return ldrInsertBytes(ldrChunkRec,lMapList,listIndex,insertList );
  end
end -- ldrInsert()


local function lmapGetLdrDigestEntry( topRec, entryItem, create_flag)

  local meth = "lmapGetLdrDigestEntry()";
  

  local lMapList = topRec[LMAP_CONTROL_BIN] ;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
  local topLdrChunk = nil; 
  GP=F and info("[ENTER]: <%s:%s> lMap(%s)", MOD, meth, tostring( lmapCtrlInfo ));
  
  if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
	local digest_bin = computeSetBin( entryItem, lmapCtrlInfo ); 
	local digestlist = lmapCtrlInfo[M_DigestList]; 
	
	GP=F and info(" <%s:%s> : Digest-entry for this index %d ",
             MOD, meth, digest_bin);
             
	if digestlist == nil then
	  -- sanity check 
	  warn("[ERROR]: <%s:%s>: Digest list nil or empty", MOD, meth);
      error('Internal Error on insert(1)');
   	end 
   	
   	GP=F and info(" <%s:%s> !!!!!!! Digest List size : %d list %s", MOD, meth, list.size( digestlist ), tostring(digestlist));
   	
	local newdigest_list = list(); 
	for i = 1, list.size( digestlist ), 1 do
	    if i == digest_bin then 
	    
	      if digestlist[i] == 0 then 
	      
	         -- What are the chances of entering this condition ??????
             GP=F and info(" <%s:%s> : Digest-entry empty for this index %d ",
             MOD, meth, digest_bin);
             GP=F and info("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
             topLdrChunk = lmapLdrListChunkCreate( topRec, lMapList ); -- create new
             lmapCtrlInfo[M_TopFull] = false; -- reset for next time.
             local newChunkDigest = record.digest( topLdrChunk );
             create_flag = true; 
           
          else 
          
            GP=F and info(" <%s:%s> : Digest-entry valid for this index %d digest(%s) ",
            MOD, meth, digest_bin, tostring( digestlist[i] ));
            topLdrChunk = aerospike:open_subrec( topRec, tostring( digestlist[i] ) );
            
          end
          
	    end -- end of digest-bin if, no concept of else, bcos this is a hash :)
	end -- end of for 
 	
  end -- end of ATOMIC check 
  
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth ); 
  return topLdrChunk; 

end --lmapGetLdrDigestEntry()

-- ======================================================================
-- lmapLdrSubRecInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the digest-list
-- dir list -- a directory of Large Data Records that will contain 
-- the data entries.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lmapList: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function lmapLdrSubRecInsert( topRec, lmapList, entryItem )
  local meth = "lmapLdrSubRecInsert()";
  
  local rc = 0;
  local lMapList = topRec[LMAP_CONTROL_BIN] ;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
  local DigestList = lmapCtrlInfo[M_DigestList];
  local digest_flag = false; 
  local topLdrChunk = nil; 
  local create_flag = true;
  
  GP=F and info("[ENTER]: !!!!!Calling <%s:%s> with DL (%s) for %s !!!!!",
  MOD, meth, tostring(lmapCtrlInfo[M_DigestList]), tostring( entryItem ));
    
   -- You have a new entry to be inserted, first go and create the LDR needed 
   -- to hold this listEntry. This also takes care of ldrPropMap and ESR creation. 
   
  local DigestListCopy = lmapCtrlInfo[M_DigestList];
   
  GP=F and info("[DEBUG]: <%s:%s> Calling for digest-list entry ", MOD, meth );
  
  topLdrChunk = lmapGetLdrDigestEntry( topRec, entryItem, create_flag); -- open existing
   
  if topLdrChunk == nil then
 	-- sanity check 
    warn("[ERROR]: <%s:%s>: topLdrChunk nil or empty", MOD, meth);
    error('Internal Error on lmapLdrListChunkCreate(1)');
  end
  local newChunkDigest = record.digest( topLdrChunk );
  GP=F and info("[DEBUG]: <%s:%s> Create flag value is %s ", MOD, meth, tostring( create_flag ) );
      
  -- HACK : TODO : Fix this number to list conversion  
  local entryList = list(); 
  list.append(entryList, entryItem); 
  
  local totalEntryCount = list.size( entryList );
  GP=F and info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  
  -- Do an ldr insert from index 1 of entryList into topLdrChunk . 
  -- In the case of lset, this is how we do the insert: 
  --  scanList( nil, tmplsetList, binList, newValue, FV_INSERT, nil, nil );
  -- For inserts, binList is just the value of topRec[binName]
    
  local countWritten = ldrInsert( topLdrChunk, lMapList, 1, entryList );
  GP=F and info(" !!!!!!! countWritten %d !!!", countWritten);
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
    error('Internal Error on insert(1)');
  end
  
  local itemsLeft = totalEntryCount - countWritten;
  -- removing the retry part of the code to attempt ldrInsert
  -- just print a warning and move-on. 
  if itemsLeft > 0 then 
  	warn("[ERROR]: <%s:%s>: Some items might not be inserted to lmap list-size : %d inserted-items : %d", 
  	      MOD, meth, list.size( entryList ),  itemsLeft);
  end 
  
  GP=F and info("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    MOD, meth, ldrChunkSummary( topLdrChunk ));

  GP=F and info("[DEBUG]: <%s:%s> Calling SUB-REC  Update ", MOD, meth );
  local status = aerospike:update_subrec( topLdrChunk );
  GP=F and info("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",MOD,meth, tostring(status));
  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC  Close ", MOD, meth );

  -- status = aerospike:close_subrec( topRec, topWarmChunk );
  status = aerospike:close_subrec( topLdrChunk );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));
    
  -- This is the part where we take the LDR list we've built and add it to the 
  -- digest list. 
  -- TODO : This needs to be moved to a separate function. 
  
  if create_flag == true then  
    GP=F and info(" <%s:%s> !!!!!! NEW LDR CREATED, update DL !!!!!!", MOD, meth );
    if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
    	local digest_bin = computeSetBin( entryItem, lmapCtrlInfo ); 
    	local digestlist = lmapCtrlInfo[M_DigestList]; 
    	
    	if digestlist == nil then
    	  -- sanity check 
    	  warn("[ERROR]: <%s:%s>: Digest list nil or empty", MOD, meth);
          error('Internal Error on insert(1)');
       	end 
       	
    	local newdigest_list = list(); 
    	for i = 1, list.size( digestlist ), 1 do
    	    if i == digest_bin then 
    	      GP=F and info(" <%s:%s> Appending digest-bin %d with digest %s for value :%s ",
                     MOD, meth, digest_bin, tostring(newChunkDigest), tostring(entryItem) );
                     
              if digestlist[i] == 0 then 
                 GP=F and info(" !!!!!!! Digest-entry empty, inserting !!!! ");
                 list.append( newdigest_list, newChunkDigest );
              end
    	      
    	    else
    	      list.append( newdigest_list, digestlist[i] );
    	    end -- end of digest_bin if 
    	end -- end of for-loop 
    	
    	lmapCtrlInfo[M_DigestList] = newdigest_list; 
    	local NewlMapList = list();
        list.append( NewlMapList, propMap );
        list.append( NewlMapList, lmapCtrlInfo );
        topRec[LMAP_CONTROL_BIN] = NewlMapList;

    end -- end of atomic check 
  end -- end of create-flag 
       
  GP=F and info("[EXIT]: !!!!!Calling <%s:%s> with DL (%s) for %s !!!!!",
  MOD, meth, tostring(lmapCtrlInfo[M_DigestList]), tostring( entryItem ));
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
  
  return rc;
 end -- lmapLdrSubRecInsert

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
-- Apply Transform Function
-- Take the Transform defined in the lsetCtrlMap, if present, and apply
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

local function simpleScanList(topRec, resultList, lMapList, binList, value, flag ) 
  local meth = "simpleScanList()";
  GP=F and info("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 MOD, meth, tostring(value), list.size(binList),
                 tostring(binList))
                 
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  
  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  
  if lmapCtrlInfo[M_Transform] ~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and info("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
    if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
      resultValue = unTransformSimpleCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          binList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lMapList[1] = propMap; 
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
    GP=F and info("[EXIT]: <%s:%s> Inserting(%s)",
                   MOD, meth, tostring(value));
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
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
local function complexScanList(topRec, lMapList, binList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];

  local transform = nil;
  local unTransform = nil;
  if lmapCtrlInfo[M_Transform]~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
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
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lMapList[1] = propMap;
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
    GP=F and info("[DEBUG]: <%s:%s> INSERTING(%s)",
                   MOD, meth, tostring(value));

    -- apply the transform (if needed)
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
    return 1 -- show caller we did an insert
  end

  GP=F and info("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
    MOD, meth, tostring(value));
  return nil;
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) resultList is nil when called for insertion 
-- (*) lMapList: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( topRec, resultList, lMapList, binList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";
  
  local lMapList =  topRec[LMAP_CONTROL_BIN];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  
  GP=F and info("[DEBUG]:<%s:%s> KeyType(%s) A(%s) C(%s)",
              MOD, meth, tostring(lmapCtrlInfo[M_KeyType]), tostring(KT_ATOMIC),
              tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
    return simpleScanList(topRec, resultList, lMapList, binList, searchValue, flag ) 
  else
    return complexScanList(topRec, resultList, lMapList, binList, searchValue, flag ) 
  end
end

-- ======================================================================
--  localInsert( topRec, lmapBinName, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- !!!!!!!! IN LMAP THIS IS CALLED ONLY IN SS_COMPACT MODE !!!!!!!!!
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) lmapBinName: The LMap control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, lmapBinName, newValue, stats )
 
  local meth = "localInsert()";
    
  GP=F and info("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));
  
  local binName = lmapBinName; 
  
  local lMapList =  topRec[LMAP_CONTROL_BIN];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  -- This binList would've been created in setupNewLmapBin()   
  local binList = topRec[binName]; 
  local insertResult = 0;
  
  if binList == nil then
  GP=F and info("[INTERNAL ERROR]:<%s:%s> binList is nil: binName(%s)",
                 MOD, meth, tostring( binName ) );
    error('Insert: INTERNAL ERROR: Nil Bin');
  else
  GP=F and info("[INTERNAL DUMP]:<%s:%s> binList is NOT nil: binName(%s)",
                 MOD, meth, tostring( binName ) );
    -- Look for the value, and insert if it is not there.
    insertResult =
      scanList( topRec, nil, lMapList, binList, newValue, FV_INSERT, nil, nil );
    -- list.append( binList, newValue );
    topRec[LMAP_CONTROL_BIN] = lMapList;
    topRec[binName] = binList; 
  end
                
  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local lMapList =  topRec[LMAP_CONTROL_BIN];
    local propMap = lMapList[1]; 
    local lmapCtrlInfo = lMapList[2];
    local itemCount = propMap[PM_ItemCount];
    local totalCount = lmapCtrlInfo[M_TotalCount];
    
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    lmapCtrlInfo[M_TotalCount] = totalCount + 1; -- Total number of items goes up
 
    local NewlMapList = list();
    list.append( NewlMapList, propMap );
    list.append( NewlMapList, lmapCtrlInfo );
    topRec[LMAP_CONTROL_BIN] = NewlMapList;
  end
 
  GP=F and info("[EXIT]: <%s:%s>Storing Record() with New Value(%s): List(%s)",
                 MOD, meth, tostring( newValue ), tostring( binList ) );
  -- No need to return anything
end -- localInsert


-- ======================================================================
-- rehashSetToLmap( topRec, lmapBinName, lmapCtrlInfo)
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
-- b. Add lmap related control-fields to lmapCtrlInfo 
-- c. Build the subrec structure needed to add a list of digests (fixed-size warm-list) 
-- d. Insert records and shove into subrecs appropriately
-- 
-- 
-- d. Add ESR 
-- e. Call subrec 
-- f. Move the current set of records into 1 warm-list structure 
-- g. Update ctrlinfo params accordingly  
-- Parms:
-- (*) topRec
-- (*) lsetBinName
-- (*) lsetCtrlMap
-- ======================================================================
local function rehashSetToLmap( topRec, lmapBinName, lmapCtrlInfo)
  local meth = "rehashSetToLmap()";
  GP=F and info("[ENTER]:<%s:%s> !!!! REHASH Mode : %s !!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ) );
  GP=F and info("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  -- If we are calling rehashSet, we probably have only one LSET list which we
  -- can access directly with name as all LMAP bins are yser-defined names. 

  local singleBinList = topRec[lmapBinName];
  if singleBinList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(singleBinName));
    error('BAD BIN 0 LIST for Rehash');
  end
  
  if lmapCtrlInfo[M_StoreState] == SS_REGULAR then
  	 -- sanity check  
     warn("[INTERNAL ERROR]:<%s:%s> Rehash can't be called in Regular mode",  
           MOD, meth);
     error('BAD Call to Rehash from regular-mode');
  end 
  
  -- Copy existing elements into temp list
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  lmapCtrlInfo[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  
  local compact_mode_flag = false; 
  local lMapList = initializeLMap( topRec, lmapBinName, compact_mode_flag );
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = lmapCtrlInfo[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- assign topRec[binName]  to LDR list
    list.append( lmapCtrlInfo[M_DigestList], 0 );
  
  end -- for each new bin
  
  for i = 1, list.size(listCopy), 1 do
      -- Now go and create the subrec structure needed to insert a digest-list
	  -- Subtle change between LSET and LMAP rehash: In the case of LSET rehash, 
	  -- we created M_Modulo LSET-bins and inserted existing Bin-0 elemnts across
	  -- all the N bins. In the case of LMAP, this now becomes a digest-list of 
	  -- entries, so we take Bin-0 elements (called by lmapBinName and not Bin-0)
	  -- and insert one LDR chunk with digest-entry. 
	  
	  -- This function does the following : 
	  -- Create and init subrec if needed
	  -- Create and init ESR if needed 
	  -- set record ldt type and prop-map
	  -- Insert existing lset list (listCopy param) items into digest list 
	  -- update top-rec, record prop-map etc 
	  -- return result. So we dont need to call localInsert() for this case
 
 	  lmapLdrSubRecInsert( topRec, lmapList, listCopy[i] ); 
  end
 
  
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );
end -- rehashSetToLmap()


local function lmapInsertRegular( topRec, lmapBinName, lMapList, newValue)

  local meth = "lmapInsertRegular()";
  
  local lMapList = topRec[LMAP_CONTROL_BIN]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local totalCount = lmapCtrlInfo[M_TotalCount];

  GP=F and info("!!!!!!: <%s:%s> Mode : %s !!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ) );
   
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT and
      totalCount >= lmapCtrlInfo[M_ThreshHold]
  then
    -- !!! Here we are switching from compact to regular mode !!!
    -- refer to lmap_design.lua for functional notes 
    GP=F and info("!!!!!!: <%s:%s> Compact Mode : %s !!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ) );
    rehashSetToLmap( topRec, lmapBinName, lmapCtrlInfo );
  else
      GP=F and info("!!!!!!: <%s:%s> Regular Mode : %s !!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ) );
      lmapLdrSubRecInsert( topRec, lmapList, newValue); 
  end
   
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );
  
end

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || AS Large Map Insert (with and without Create)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Insert a value into the set.
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
-- (*) lmapBinName: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- (*) createSpec: When in "Create Mode", use this Create Spec
local function localLMapInsert( topRec, lmapBinName, newValue, createSpec )
  local meth = "localLMapInsert()";
	  
  GP=F and info("[ENTER]:<%s:%s> SetBin(%s) NewValue(%s) createSpec(%s)",
                 MOD, meth, tostring(lmapBinName), tostring( newValue ),
                 tostring( createSpec ));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateLmapParams( topRec, lmapBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[LMAP_CONTROL_BIN] == nil ) then
    warn("!!!!!!: <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );
         
    -- we are going to start off in compact mode. 
    local compact_mode_flag = true; 
    local lMapList = initializeLMap( topRec, lmapBinName, compact_mode_flag );
    local propMap = lMapList[1]; 
    local lmapCtrlInfo = lMapList[2]; 

    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
    adjustLMapCtrlInfo( lmapCtrlInfo, createSpec );
    -- Changes to the map need to be re-appended to topRec  
    local NewLmapList = list();
    list.append( NewLmapList, propMap );
    list.append( NewLmapList, lmapCtrlInfo );
    topRec[LMAP_CONTROL_BIN] = NewLmapList;
    
    GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after adjustLMapCtrlInfo(%s)",
      MOD, meth , lmapSummaryString(NewLmapList));
    end
         
    -- initializeLMap always sets lMapCtrlInfo.StoreState to SS_COMPACT
    -- At this point there is only one bin.
    -- This one will assign the actual record-list to topRec[binName]
    setupNewLmapBin( topRec, lmapBinName );
  
  end

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local lMapList = topRec[LMAP_CONTROL_BIN]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local totalCount = lmapCtrlInfo[M_TotalCount];
  
  -- In the case of LMAP, we call localInsert only if it is SS_COMPACT mode
  -- insertion of elements into the first LMAP bin like an lset-insert. If not
  -- rehashSettoLmap will take care of the insertion as well. Please refer to
  -- notes mentioned in rehashSettoLmap() about these differences. 
  
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT and 
         totalCount < lmapCtrlInfo[M_ThreshHold] then
    -- !!! we are safely in compact mode !!!! 
    GP=F and info("localInsert() for LMAP INSERT Count %d Threshold : %d ",
    			totalCount, tostring( lmapCtrlInfo[M_ThreshHold] ) );
    localInsert( topRec, lmapBinName, newValue, 1 );
  else
  	lmapInsertRegular( topRec, lmapBinName, lMapList, newValue); 
  end
  
   -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and info("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc
  
end -- function localLMapInsert()



-- ======================================================================
-- lmap_insert() -- with and without create
-- ======================================================================
function lmap_insert( topRec, lmapBinName, newValue )
  return localLMapInsert( topRec, lmapBinName, newValue, nil )
end -- lmap_insert()

function lmap_create_and_insert( topRec, lmapBinName, newValue, createSpec )
  return localLMapInsert( topRec, lmapBinName, newValue, createSpec )
end -- lmap_create_and_insert()
