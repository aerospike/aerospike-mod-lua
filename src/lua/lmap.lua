-- Large Map Operations
-- lmap.lua:  August 14, 2013
--
-- Module Marker: Keep this in sync with the stated version
local MOD="lmap_2013_08_14.a"; -- the module name used for tracing

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
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print

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

-- AS LMAP Bin Names
local LMAP_CONTROL_BIN       = "LMapCtrlBin";

-- ++===============++
-- || Package Names ||
-- ++===============++
local PackageStumbleUpon     = "StumbleUpon";

-- Standard, Test and Debug Packages
local PackageStandardList    = "StandardList";
-- Test Modes
local PackageTestModeObject  = "TestModeObject";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
-- Debug Modes
local PackageDebugModeObject = "DebugModeObject";
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
local M_Modulo                 = 'O';
local M_ThreshHold             = 'H';
local M_KeyFunction            = 'K'; -- User Supplied Key Extract Function
local M_CompactNameList        = 'Q';--Simple Compact List -- before "tree mode"
local M_CompactValueList       = 'v';--Simple Compact List -- before "tree mode"

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
-- A:                         a:                        0:
-- B:                         b:M_LdrByteCountMax       1:
-- C:                         c:                        2:
-- D:                         d:                        3:
-- E:                         e:M_LdrEntryCountMax      4:
-- F:M_TopFull                f:                        5:
-- G:                         g:                        6:
-- H:M_Threshold              h:                        7:
-- I:                         i:                        8:
-- J:                         j:                        9:
-- K:                         k:                  
-- L:                         l:M_ListDigestCount
-- M:M_StoreMode              m:
-- N:                         n:
-- O:                         o:
-- P:                         p:
-- Q:                         q:
-- R:M_ColdDataRecCount       r:
-- S:M_StoreLimit             s:M_LdrByteEntrySize
-- T:                         t:M_Transform
-- U:                         u:M_UnTransform
-- V:                         v:
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
local LDR_CTRL_BIN = "LdrControlBin";  
local LDR_NLIST_BIN = "LdrNListBin";  
local LDR_VLIST_BIN = "LdrVListBin";  
local LDR_BNRY_BIN = "LdrBinaryBin";

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
  GP=F and info("[ENTER]<%s:%s>", MOD, meth );

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

  GP=F and info("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return srcList;
end -- createSubrecContext()

-- ======================================================================
-- Given an already opened subrec (probably one that was recently created),
-- add it to the subrec context.
-- ======================================================================
local function addSubrecToContext( srcList, subrec )
  local meth = "addSubrecContext()";
  GP=F and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring( srcList));

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

  GP=F and info("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- openSubrec()
-- ======================================================================
local function openSubrec( srcList, topRec, digestString )
  local meth = "openSubrec()";
  GP=F and info("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
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

  GP=F and info("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
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
  GP=F and info("[ENTER]<%s:%s> DigestStr(%s) SRC(%s)",
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

  info("[STATUS]<%s:%s> Closing Rec: Digest(%s)", MOD, meth, digestString);

  if( dirtyStatus == true ) then
    warn("[WARNING]<%s:%s> Can't close Dirty Record: Digest(%s)",
      MOD, meth, digestString);
  else
    rc = aerospike:close_subrec( subrec );
    GP=F and info("[STATUS]<%s:%s>Closed Rec: Digest(%s) rc(%s)", MOD, meth,
      digestString, tostring( rc ));
  end

  GP=F and info("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
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
  --GP=F and info("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
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

  GP=F and info("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- updateSubrec()

-- ======================================================================
-- markSubrecDirty()
-- ======================================================================
local function markSubrecDirty( srcList, digestString )
  local meth = "markSubrecDirty()";
  GP=F and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  -- Pull up the dirtyMap, find the entry for this digestString and
  -- mark it dirty.  We don't even care what the existing value used to be.
  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  dirtyMap[digestString] = true;
  
  GP=F and info("[EXIT]<%s:%s> SRC(%s)", MOD, meth, tostring(srcList) );
  return 0;
end -- markSubrecDirty()

-- ======================================================================
-- closeAllSubrecs()
-- ======================================================================
local function closeAllSubrecs( srcList )
  local meth = "closeAllSubrecs()";
  GP=F and info("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

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

  GP=F and info("[EXIT]: <%s:%s> : RC(%s)", MOD, meth, tostring(rc) );
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
local function getKeyValue( lmapCtrlInfo, value )
  local meth = "getKeyValue()";
  GP=F and info("[ENTER]<%s:%s> value(%s)",
       MOD, meth, tostring(value) );

  GP=F and info(" Ctrl-Map : %s", tostring(lmapCtrlInfo));

  local keyValue;
  if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
    keyValue = value;
  else
    -- Employ the user's supplied function (keyFunction) and if that's not
    -- there, look for the special case where the object has a field
    -- called 'key'.  If not, then, well ... tough.  We tried.
    local keyFunction = lmapCtrlInfo[M_KeyFunction];

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

  GP=F and info("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
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
  GP=F and info("[ENTER]: <%s:%s> transform(%s) type(%s) Value(%s)",
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
local function applyUnTransform( lmapCtrlInfo, storeValue )
  local returnValue = storeValue;
  if lmapCtrlInfo[M_UnTransform] ~= nil and
    functionTable[lmapCtrlInfo[M_UnTransform]] ~= nil then
    returnValue = functionTable[lmapCtrlInfo[M_UnTransform]]( storeValue );
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
local function unTransformComplexCompare(lmapCtrlInfo, unTransform, dbValue, searchKey)
  local meth = "unTransformComplexCompare()";

  GP=F and info("[ENTER]: <%s:%s> unTransform(%s) dbVal(%s) key(%s)",
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
  resultMap.StoreMode            = lmapCtrlInfo[M_StoreMode];
  resultMap.Transform            = lmapCtrlInfo[M_Transform];
  resultMap.UnTransform          = lmapCtrlInfo[M_UnTransform];
  resultMap.KeyCompare           = lmapCtrlInfo[M_KeyCompare];
  resultMap.BinaryStoreSize      = lmapCtrlInfo[M_BinaryStoreSize];
  resultMap.KeyType              = lmapCtrlInfo[M_KeyType];
  resultMap.TotalCount	         = lmapCtrlInfo[M_TotalCount];		
  resultMap.Modulo 		 = lmapCtrlInfo[M_Modulo];
  resultMap.ThreshHold		 = lmapCtrlInfo[M_ThreshHold];
  
  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = lmapCtrlInfo[M_LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = lmapCtrlInfo[M_LdrByteEntrySize];
  resultMap.LdrByteCountMax      = lmapCtrlInfo[M_LdrByteCountMax];

  -- Digest List Settings: List of Digests of LMAP Data Records
  -- specific to LMAP in STANDARD_MODE ONLY 
  
  resultMap.DigestList        = lmapCtrlInfo[M_DigestList];
  resultMap.TopFull 	      = lmapCtrlInfo[M_TopFull];
  resultMap.ListDigestCount   = lmapCtrlInfo[M_ListDigestCount];
  resultMap.ListMax           = lmapCtrlInfo[M_ListMax];
  resultMap.TopChunkByteCount = lmapCtrlInfo[M_TopChunkByteCount];
  resultMap.TopChunkEntryCount= lmapCtrlInfo[M_TopChunkEntryCount];

  return resultMap;
end -- lmapSummary()

-- ======================================================================
-- Make it easier to use lsoSummary(): Have a String version.
-- ======================================================================
local function lmapSummaryString( lmapList )
    return tostring( lmapSummary( lmapList ) );
end

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
      GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 

  GP=F and info("[EXIT]<%s:%s> rc(%d)", MOD, meth, rc );
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
-- initializeLMap:
-- ======================================================================
-- Set up the LMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LMAP BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LMAP
-- behavior.  Thus this function represents the "type" LMAP -- all
-- LMAP control fields are defined here.
-- The LMap is obtained using the user's LMap Bin Name:
-- ======================================================================

local function initializeLMap( topRec, lmapBinName )
  local meth = "initializeLMap()";
  
  -- Create 2 maps : The generic property map 
  -- and lmap specific property map. Create one
  -- list : the actual LDR list for lmap. 
  -- Note: All Field Names start with UPPER CASE.
  local lmapCtrlInfo = map();
  local propMap = map(); 
  local lmapList = list(); 
  
  
  GP=F and info("[ENTER]: <%s:%s>:: LMapBinName(%s)",
  MOD, meth, tostring(lmapBinName));
  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM_ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM_SubRecCount] = 0;
  propMap[PM_Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM_LdtType]    = LDT_TYPE_LMAP; -- Validate the ldt type
  propMap[PM_Magic]      = MAGIC; -- Special Validation
  propMap[PM_BinName]    = lmapBinName; -- Defines the LSO Bin
  propMap[PM_RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM_EsrDigest]  = nil; -- not set yet.
  propMap[PM_SelfDigest] = nil; 
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
  lmapCtrlInfo[M_CompactNameList]    = list(); -- name-entries of name-value pair in lmap to be held in compact mode 
  lmapCtrlInfo[M_CompactValueList]   = list(); -- value-entries of name-value pair in lmap to be held in compact mode 
	  
  -- Put our new maps in a list, in the record, then store the record.
  list.append( lmapList, propMap );
  list.append( lmapList, lmapCtrlInfo );
  
  setLdtRecordType( topRec );

  -- Once this list of 2 maps is created, we need to assign it to topRec
  topRec[lmapBinName]            = lmapList;
  record.set_flags(topRec, lmapBinName, BF_LDT_BIN );--Must set every time
  

  GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after Init(%s)",
      MOD, meth , lmapSummaryString(lmapList));

  GP=F and info("[EXIT]:<%s:%s>:", MOD, meth );
  return lmapList;
  
end -- initializeLMap

local function initializeLMapRegular(topRec, lmapBinName)
  local meth = "initializeLMapRegular()";
  
  GP=F and info("[ENTER]: <%s:%s>:: Regular Mode LMapBinName(%s)",
  MOD, meth, tostring(lmapBinName));
  
  local lMapList = topRec[lmapBinName] ; -- The main lsoMap structure

  -- Extract the property map and lso control map from the lso bin list.
  local propMap = lMapList[1];
  local lmapCtrlInfo  = lMapList[2];
  
  -- we are now in rehashSettoLmap(), we need to change lmapCtrlIndo params  
  -- all the other params must already be set by default. 
 
  GP=F and info("[ENTER]: <%s:%s>:: Regular-Mode LMapBinName(%s) Key-type: %s",
      MOD, meth, tostring(lmapBinName), tostring(lmapCtrlInfo[M_KeyType]));

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

  -- Do we need this topRec assignment here ?
  -- TODO : Ask Toby about it 
 
  topRec[lmapBinName] = lMapList;
  record.set_flags(topRec, lmapBinName, BF_LDT_BIN );--Must set every time
  GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after Init(%s)",
       MOD, meth , lmapSummaryString(lMapList));

  GP=F and info("[EXIT]:<%s:%s>:", MOD, meth );
  
end 

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateLmapBinName( binName )
  local meth = "validateBinName()";
  GP=F and info("[ENTER]: <%s:%s> validate Bin Name(%s)",
      MOD, meth, tostring(binName));

  if binName == nil  then
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( binName ) ~= "string"  then
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( binName ) > 14 then
    error( ldte.ERR_BIN_NAME_TOO_LONG );
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
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
     
    -- Control Bin Must Exist
    if( topRec[lmapBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LMAP BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(lmapBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    local lMapList = topRec[lmapBinName] ; -- The main lsoMap structure

    -- Extract the property map and lso control map from the lso bin list.
    local propMap = lMapList[1];
    local lMapCtrlInfo  = lMapList[2];

    if propMap[PM_Magic] ~= MAGIC then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LMAP BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( lmapBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    
    if topRec ~= nil and topRec[lmapBinName] ~= nil then
      local lMapList = topRec[lmapBinName]; -- The main lsoMap structure
      -- Extract the property map and lso control map from the lso bin list.
      local propMap = lMapList[1];
      local lMapCtrlInfo  = lMapList[2];
      if propMap[PM_Magic] ~= MAGIC then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LMAP BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( lmapBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
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
 
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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
 
  GP=F and info(" packageTestModeObject Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( lmapCtrlInfo )
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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
 
end -- packageTestModeNumber()

-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
local function packageTestModeObject( lmapCtrlInfo )
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Atomic Keys
--  lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted
  --lmapCtrlInfo[M_KeyFunction] = "keyExtract"; -- Defined in UdfFunctionTable
  lmapCtrlInfo[M_KeyFunction] = nil; 
  GP=F and info(" packageTestModeObject Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
  
 end -- packageTestModeObject()

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lmapCtrlInfo )
  
  GP=F and info("Enter !!! TEST MODE LIST !!!!!!!!!!!!!"); 
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted
  -- lmapCtrlInfo[M_KeyFunction] = "keyExtract"; -- Defined in UdfFunctionTable
  lmapCtrlInfo[M_KeyFunction] = nil; 
 
  GP=F and info(" packageTestModeList Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lmapCtrlInfo )
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
  -- General Parameters
  lmapCtrlInfo[M_Transform] = "compressTest4";
  lmapCtrlInfo[M_UnTransform] = "unCompressTest4";
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
  --lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted

  GP=F and info(" packageTestModeBinary Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );

end -- packageTestModeBinary( lmapCtrlInfo )

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSET with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
local function packageDebugModeObject( lmapCtrlInfo )
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
  -- General Parameters
  lmapCtrlInfo[M_Transform] = nil;
  lmapCtrlInfo[M_UnTransform] = nil;
  lmapCtrlInfo[M_StoreState] = SS_COMPACT; -- start in "compact mode"
  lmapCtrlInfo[M_StoreMode] = SM_LIST; -- Use List Mode
  lmapCtrlInfo[M_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
  lmapCtrlInfo[M_KeyType] = KT_COMPLEX; -- Atomic Keys
--  lmapCtrlInfo[M_BinName] = LSET_CONTROL_BIN;
  lmapCtrlInfo[M_Modulo] = DEFAULT_DISTRIB;
  lmapCtrlInfo[M_ThreshHold] = 4; -- Rehash after this many have been inserted
  -- lmapCtrlInfo[M_KeyFunction] = "keyExtract"; -- Defined in UdfFunctionTable
  lmapCtrlInfo[M_KeyFunction] = nil; 
  
  GP=F and info(" packageDebugModeObject Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
end -- packageDebugModeObject()

-- ======================================================================
-- Package = "StumbleUpon"
-- StumbleUpon uses a compacted representation.
-- NOTE: This will eventually move to the UDF Function Table, or to a
-- separate Configuration file.  For the moment it is included here for
-- convenience. 
-- ======================================================================
local function packageStumbleUpon( lmapCtrlInfo )
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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

  GP=F and info(" packageDebugModeBinary Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
end -- packageDebugModeBinary( lmapCtrlInfo )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( lmapCtrlInfo )
  local meth = "packageDebugModeNumber()";
  GP=F and info("[ENTER]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring( lmapCtrlInfo ));
  
  GP=F and info("Enter !!!!!!!!!!!!!!!!!"); 
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

  GP=F and info("[EXIT]: <%s:%s>:: CtrlMap(%s)",
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
  GP=F and info("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

  for name, value in map.pairs( argListMap ) do
    GP=F and info("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s) TYPE : %s",
        MOD, meth, tostring( name ), tostring( value ), type(value));

    -- Process our "prepackaged" settings first:
    -- NOTE: Eventually, these "packages" will be installed in either
    -- a separate "package" lua file, or possibly in the UdfFunctionTable.
    -- Regardless though -- they will move out of this main file, except
    -- maybe for the "standard" packages.

    if name == "Package" and type( value ) == "string" then
      -- Figure out WHICH package we're going to deploy:
      GP=F and info("Enter !!!!!!!!!!!!!!!!! Name: %s Value: %s::%s", tostring(name), tostring(value), value ); 

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
    elseif value == PackageTestModeObject then
        packageTestModeObject( lmapCtrlInfo );
    elseif value == PackageDebugModeObject then
        packageDebugModeObject( lmapCtrlInfo );
    else
	warn(" <>><><><><>< UNKNOWN PACKAGE <><><><> (%s)", tostring(value) );
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

  GP=F and info("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s) Threshold : %s",
    MOD, meth , tostring(lmapCtrlInfo), tostring(lmapCtrlInfo[M_ThreshHold]));
      
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

  -- TODO: This looks really wrong -- we're not calling this function, are we?

  -- TODO : Code for standard mode
  return 0;
end -- setupNewLmapBin

-- This gets called after every lmap_create to set the self-digest and update 
-- TODO : Ask Toby if this can be done in another way 
-- DONE : You just need to assign the lMapList back into the record, but you
--        do NOT need to create a new lMapList.
local function lmap_update_topdigest( topRec, binName )
    local meth = "lmap_update_topdigest()";
    local lMapList = topRec[binName] ;
    local propMap = lMapList[1]; 
    local lmapCtrlInfo = lMapList[2];
    propMap[PM_SelfDigest]   = record.digest( topRec );

    topRec[binName] = lMapList;
    record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time

    rc = aerospike:update( topRec );
    if( rc == nil or rc == 0 ) then
      GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 
    GP=F and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
    return rc;
end

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
  GP=F and info("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
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
  GP=F and info("[ENTER]: <%s:%s> val(%s) type = %s Map(%s) ",
                 MOD, meth, tostring(newValue), type(newValue), tostring(lmapCtrlInfo) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local binNumber  = 0;
  local key = 0; 
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then
    -- In the case of LMAP, we dont need to worry about this
    -- because we never call this for compact
    return 0
  else
    if( lmapCtrlInfo[M_KeyType] == KT_ATOMIC ) then
      key = newValue;
      GP=F and info(" Type of Key ATOMIC = %s", type(key))
    else
      -- WE ARE DEALING WITH NAME VALUE PAIRS HERE
      -- SO THE KEY WILL BE BASED ON THE STRING OF THE 
      -- THE KEY/NAME FIELD, IF A KEY-FUNCTION IS NOT SPECIFIED  
      local key = getKeyValue( lmapCtrlInfo, newValue );
    end

    if type(key) == "number" then
      binNumber  = numberHash( key, lmapCtrlInfo[M_Modulo] );
    elseif type(key) == "string" then
      binNumber  = stringHash( key, lmapCtrlInfo[M_Modulo] );
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type %s (should be number, string or map)",
           MOD, meth, type(key) );
      error( ldte.ERR_INTERNAL );
    end
  end
  
  local digestlist = lmapCtrlInfo[M_DigestList]
  GP=F and info("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) Entry : %s",
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
local function createAndInitESR( topRec, lmapBinName)
  local meth = "createAndInitESR()";
  GP=F and info("[ENTER]: <%s:%s>", MOD, meth );

  local lMapList = topRec[lmapBinName] ;
  local propMap = lMapList[1]; 
  -- local lmapCtrlInfo = lMapList[2]; Not needed here
  
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
  info("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esr, RT_ESR );
  info("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );
  
  esr[SUBREC_PROP_BIN] = esrPropMap;

  GP=F and info("[DEBUG]: <%s:%s> Leaving with ESR Digest(%s): EsrMap(%s)",
    MOD, meth, tostring(esrDigest), tostring( esrPropMap));

  -- no need to use updateSubrec for this, we dont need 
  -- maintain accouting for ESRs. 
  
  rc = aerospike:update_subrec( esr );
  if( rc == nil or rc == 0 ) then
    info("DO NOT CLOSE THE ESR FOR NOW");
      -- aerospike:close_subrec( esr );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  -- update global attributes. 
  propMap[PM_EsrDigest] = esrDigest; 
  
  -- local NewlMapList = list();
  -- list.append( NewlMapList, propMap );
  -- list.append( NewlMapList, lmapCtrlInfo );
  
  -- If the topRec already has an REC_LDT_CTRL_BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- setLdtRecordType( topRec );
  topRec[lmapBinName] = lMapList;
  record.set_flags(topRec, lmapBinName, BF_LDT_BIN ); -- propMap has been updated 

  -- Now that it's initialized, add the ESR to the SRC.
  -- addSubrecToContext( src, esr );
  GP=F and info("[DEBUG]<%s:%s>Validate lMapList Contents(%s)",
    MOD, meth, tostring( lMapList ));

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
-- (*) ldrRec[LDR_NLIST_BIN]: The Name Entry List (when in list mode)
-- (*) ldrRec[LDR_VLIST_BIN]: The Value Entry List (when in list mode)
-- (*) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
--
-- When we call this method, we have just created a LDT SubRecord.  Thus,
-- we must check to see if that is the FIRST one, and if so, we must also
-- create the Existence Sub-Record for this LDT.
-- ======================================================================

local function initializeSubrecLdrMap( topRec, lmapBinName, newLdrChunkRecord, ldrPropMap, ldrMap )
  local meth = "initializeSubrecLdrMap()";
  GP=F and info("[ENTER]: <%s:%s> Name: TopRec: ", MOD, meth );

  local lMapList = topRec[lmapBinName] ;
  local propMap = lMapList[1];
  local lmapCtrlInfo = lMapList[2];
  local binName    = lmapBinName; 

  -- topRec's digest is the parent digest for this subrec 
  ldrPropMap[PM_ParentDigest] = record.digest( topRec );
  -- Subrec's (its own) digest is the selfDigest :)
  ldrPropMap[PM_SelfDigest]   = record.digest( newLdrChunkRecord ); 
  ldrPropMap[PM_Magic]        = MAGIC;
  ldrPropMap[PM_RecType]   = RT_SUB;
  
  --  Use Top level LMAP entry for mode and max values
  ldrMap[LDR_ByteEntrySize]   = lmapCtrlInfo[M_LdrByteEntrySize];
  ldrMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries
  
  -- If this is the first LDR, then it's time to create an ESR for this
  -- LDT. There is one ESR created per LMAP bin, not per LDR chunk creation.
  if( propMap[PM_EsrDigest] == nil or ldrPropMap[PM_EsrDigest] == 0 ) then
    GP=F and info(" !!!!!!!!!!! First ESR creation for LDT bin !!!!!!!!!!!!!!!");
    ldrPropMap[PM_EsrDigest] = createAndInitESR( topRec, lmapBinName );
  end

  -- Double checking the assignment -- this should NOT be needed, as the
  -- caller does it right after return of this function.
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;

  -- Set the type of this record to LDT (it might already be set by another
  -- LDT in this same record).
  record.set_type( newLdrChunkRecord, RT_SUB ); -- LDT Type Rec
end -- initializeSubrecLdrMap()

-- ======================================================================
-- lmapLdrListChunkCreate( src, topRec, lMapList )
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

local function lmapLdrListChunkCreate( src, topRec, lmapBinName )
  local meth = "lmapLdrListChunkCreate()";

  GP=F and info("[ENTER]: <%s:%s> Bin-Name: %s", MOD, meth, tostring(lmapBinName) );
  
  -- TODO : we need to add a check to even see if we can accomodate any more 
  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.

  local newLdrChunkRecord = aerospike:create_subrec( topRec );
  
  if newLdrChunkRecord == nil then 
    warn("[ERROR]<%s:%s>Problems Creating Subrec New-entry(%s)",
      MOD,meth,tostring(newLdrChunkRecord));
    error( ldte.ERR_SUBREC_CREATE );
  end
  
  local lMapList = topRec[lmapBinName] ;
  local ldrPropMap = map();
  local ldrMap = map();
  local newChunkDigest = record.digest( newLdrChunkRecord );
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName    = lmapBinName; 

  -- Update the subrec count (and remember to save the change)
  local subrecCount = propMap[PM_SubRecCount];
  propMap[PM_SubRecCount] = subrecCount + 1;
  local rc = addSubrecToContext( src, newLdrChunkRecord ); 
  
  -- Each subrec that gets created, needs to have its properties initialized. 
  -- Also the ESR structure needs to get created, if needed
  -- Plus the REC_LDT_CTRL_BIN of topRec needs to be updated. 
  -- This function takes care of doing all of that. 
  
  initializeSubrecLdrMap( topRec, lmapBinName, newLdrChunkRecord, ldrPropMap, ldrMap );

  -- Assign Prop, Control info and List info to the LDR bins
  newLdrChunkRecord[SUBREC_PROP_BIN] = ldrPropMap;
  newLdrChunkRecord[LDR_CTRL_BIN] = ldrMap;
  newLdrChunkRecord[LDR_NLIST_BIN] = list();
  newLdrChunkRecord[LDR_VLIST_BIN] = list();

  GP=F and info("[EXIT]: <%s:%s> ldrPropMap(%s) Name-list: %s value-list: %s ",
    MOD, meth, tostring( ldrPropMap ), tostring(newLdrChunkRecord[LDR_NLIST_BIN]), tostring(newLdrChunkRecord[LDR_VLIST_BIN]));

  GP=F and info("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrPropMap) );
  
  -- Add our new chunk (the digest) to the DigestList
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
   GP=F and info("[DEBUG]: <%s:%s> Appending NewChunk %s with digest(%s) to DigestList(%s)",
    MOD, meth, tostring(newLdrChunkRecord), tostring(newChunkDigest), tostring(lmapCtrlInfo[M_DigestList]));

  GP=F and info("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LMap(%s): ",
    MOD, meth, tostring(newChunkDigest), tostring(lmapCtrlInfo));
   
  -- Increment the Digest Count
  -- gets inceremented once per LDR entry add. 
  local ChunkCount = lmapCtrlInfo[M_ListDigestCount]; 
  lmapCtrlInfo[M_ListDigestCount] = (ChunkCount + 1);

  -- This doesn't appear to be getting set (updated) anywhere else.
  -- Do it here.
  aerospike:update_subrec( newLdrChunkRecord );

  GP=F and info("[EXIT]: <%s:%s> ldrPropMap(%s) Name-list: %s value-list: %s ",
    MOD, meth, tostring( ldrPropMap ), tostring(newLdrChunkRecord[LDR_NLIST_BIN]), tostring(newLdrChunkRecord[LDR_VLIST_BIN]));
  
  return newLdrChunkRecord;
end --  lmapLdrListChunkCreate()

-- ======================================================================
-- ldrInsertList( topLdrChunk, lMapList, listIndex, nameList, valueList )
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

local function ldrInsertList(ldrChunkRec,lMapList,listIndex,nameList,valueList )
  local meth = "ldrInsertList()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
 
   if ldrChunkRec == nil then
 	-- sanity check 
    warn("[ERROR]: <%s:%s>: ldrChunkRec nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
  else
  	GP=F and info(" LDRCHUNKREC not nil <%s:%s>  ", MOD, meth);
  end

  -- These 2 get assigned in lmapLdrListChunkCreate() to point to the ctrl-map. 
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
  local itemSlotsAvailable = (lmapCtrlInfo[M_LdrEntryCountMax] - chunkNameIndexStart) + 1;

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
   
  GP=F and info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()

-- ======================================================================
-- ldrInsertBytes( topLdrChunk, lMapList, listIndex, nameList, valueList )
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
local function ldrInsertBytes( ldrChunkRec, lMapList, listIndex, nameList, valueList )
  local meth = "ldrInsertBytes()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];

  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];
  GP=F and info("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = lmapCtrlInfo[M_LdrByteEntrySize];
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", MOD, meth);
    -- Let the caller handle the error.
    error( ldte.ERR_INTERNAL );
  end

  local entryCount = 0;
  if( ldrMap[LDR_ByteEntryCount] ~= nil and
      ldrMap[LDR_ByteEntryCount] ~= 0 )
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
    GP=F and info("[DEBUG]:<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));
  else
    GP=F and
    info("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec[LDR_BNRY_BIN]));

    -- The API for this call changed (July 2, 2013).  Now use "ensure"
    -- bytes.set_len(ldrChunkRec[LDR_BNRY_BIN], totalSpaceNeeded );
    bytes.ensure(ldrChunkRec[LDR_BNRY_BIN], totalSpaceNeeded, 1);

    GP=F and
    info("[DEBUG]:<%s:%s>AFTER: Extending BYTES: New Size(%d) ByteArray(%s)",
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
    info("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
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
local function ldrInsert(ldrChunkRec,lMapList,listSize, nameList, valueList )
  local meth = "ldrInsert()";

  GP=F and info("[ENTER]: <%s:%s> list-size(%d) NameList(%s), valueList(%s), ChunkSummary(%s)",
    MOD, meth, listSize, tostring( nameList ), tostring( valueList ), tostring(ldrChunkRec));
    
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];

  if lmapCtrlInfo[M_StoreMode] == SM_LIST then
    return ldrInsertList(ldrChunkRec,lMapList,listSize,nameList,valueList);
  else
    return ldrInsertBytes(ldrChunkRec,lMapList,listSize,nameList,valueList);
  end

end -- ldrInsert()


local function lmapGetLdrDigestEntry( src, topRec, lmapBinName, entryItem, create_flag)

  local meth = "lmapGetLdrDigestEntry()";
  

  local lMapList = topRec[lmapBinName] ;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
  local topLdrChunk = nil; 
  GP=F and info("[ENTER]: <%s:%s> lMap(%s)", MOD, meth, tostring( lmapCtrlInfo ));
  
  local digest_bin = computeSetBin( entryItem, lmapCtrlInfo ); 
  local digestlist = lmapCtrlInfo[M_DigestList]; 
	
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
         topLdrChunk = lmapLdrListChunkCreate( src, topRec, lmapBinName ); -- create new
         lmapCtrlInfo[M_TopFull] = false; -- reset for next time.
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
  
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth ); 
  return topLdrChunk; 

end --lmapGetLdrDigestEntry()

local function lmapCheckDuplicate(lmapCtrlInfo, ldrChunkRec, entryItem)
  
  local flag = false; 
  if lmapCtrlInfo[M_StoreMode] == SM_LIST then
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
-- (*) lmapList: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function lmapLdrSubRecInsert( src, topRec, lmapBinName, newName, newValue)
  local meth = "lmapLdrSubRecInsert()";
  
  local rc = 0;
  local lMapList =  topRec[lmapBinName] ;
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = lmapBinName; 
  local DigestList = lmapCtrlInfo[M_DigestList];
  local digest_flag = false; 
  local topLdrChunk = nil; 
  local create_flag = true;
  
  GP=F and info("[ENTER]: !!!!!Calling <%s:%s> with DL (%s) for Name-Value pair %s:%s !!!!!",
  MOD, meth, tostring(lmapCtrlInfo[M_DigestList]), tostring( newName ), tostring( newValue ));
    

  -- You have a new entry to be inserted, first go and create the LDR needed 
  -- to hold this listEntry. This also takes care of ldrPropMap and ESR creation. 
   
  local DigestListCopy = lmapCtrlInfo[M_DigestList];
  
  -- In the name-value pair in lmap, the name acts as the key !!!!!!!!!! 
  -- This function creates a subrec if the entry is empty, returns open chunk 
  topLdrChunk = lmapGetLdrDigestEntry( src, topRec, lmapBinName, newName, create_flag); 
   
  if topLdrChunk == nil then
 	-- sanity check 
    warn("[ERROR]: <%s:%s>: topLdrChunk nil or empty", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end
  
  local newChunkDigest = record.digest( topLdrChunk );
 
  GP=F and info("[DEBUG]: <%s:%s> LDR chunk Name-list:%s, value-list:%s ",
		 MOD, meth, tostring( topLdrChunk[LDR_NLIST_BIN] ), tostring( topLdrChunk[LDR_VLIST_BIN] ) );
      
  -- Before we try to do insert, lets take care of duplicates using name/key
  local exists_flag = lmapCheckDuplicate(lmapCtrlInfo, topLdrChunk, newName); 
  
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
    
  local countWritten = ldrInsert( topLdrChunk, lMapList, 1, nameList, valueList );
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
  local totalCount = lmapCtrlInfo[M_TotalCount];
  propMap[PM_ItemCount] = itemCount + countWritten; -- number of valid items goes up
  lmapCtrlInfo[M_TotalCount] = totalCount + countWritten; -- Total number of items goes up
  
  
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
    local digest_bin = computeSetBin( newName, lmapCtrlInfo ); 
    local digestlist = lmapCtrlInfo[M_DigestList]; 
    
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
    
    lmapCtrlInfo[M_DigestList] = newdigest_list; 
    topRec[lmapBinName] = lMapList;
    record.set_flags(topRec, lmapBinName, BF_LDT_BIN );--Must set every time

    rc = aerospike:update( topRec );

    if( rc == nil or rc == 0 ) then
      GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end 

  end -- end of create-flag 
       
  GP=F and info("[EXIT]: !!!!!Calling <%s:%s> with DL (%s) for %s !!!!!",
  MOD, meth, tostring(lmapCtrlInfo[M_DigestList]), tostring( entryItem ));
  local digestlist = lmapCtrlInfo[M_DigestList]; 
  GP=F and info(" DigestList %s Size: %s", tostring(digestlist), tostring(list.size(digestlist)));
  
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
  
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

local function simpleScanList( topRec, lmapBinName, resultList, newName, newValue, 
       flag, filter, fargs )

  local meth = "simpleScanList()";

  GP=F and info("[ENTER]: <%s:%s> Name-List(%s), Value-List(%s)",
                 MOD, meth, tostring(nameList), 
                 tostring(valueList));
                 
  local lMapList =  topRec[lmapBinName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  
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

  -- In LMAP with name-value pairs, all operations are done 
  -- based on key-comparison. So we will parse name-list 
  -- to do everything !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  local resultValue = nil;

  for i = 1, list.size( nameList ), 1 do
    GP=F and info("[DEBUG]: <%s:%s> Comparing Name-entry(%s) with Name-list(%s)",
                   MOD, meth, tostring(newName), tostring(nameList[i]));

    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
    if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
      -- CHECK IF THE KEY/NAME IS PRESENT, transform function applicable only to value  
      resultValue = unTransformSimpleCompare(nil, nameList[i], newName);
      if resultValue ~= nil then

        GP=F and info("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));

	-- Return point for FV_DELETE and FV_SCAN, what should we return here ?
        -- if its a search, the result should return the pair in the name-value pair
	-- if its a deleted, the result should be just the return code and the 
        -- result-list will be nil !!!!!!!!!!!!!!!!!!!!

        if( flag == FV_DELETE ) then
          local newString = nameList[i]..":"..valueList[i];  
          list.append( resultList, newString );
          nameList[i] = FV_EMPTY; -- the name-entry is NO MORE
          valueList[i] = FV_EMPTY; -- the value-entry is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lMapList[1] = propMap; 
          lmapCtrlInfo[M_CompactNameList] = nameList; 
          lmapCtrlInfo[M_CompactValueList] = valueList; 
          lMapList[2] = lmapCtrlInfo;
          topRec[lmapBinName] = lMapList; 
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
          local newString = nameList[i]..":"..resultFiltered; 
          list.append( resultList, newString );
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
    GP=F and info("[EXIT]: <%s:%s> Inserting(%s)",
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
local function complexScanList( topRec, lmapBinName, resultList, newName, newValue, 
       flag, filter, fargs )

  local meth = "complexScanList()";
  local lMapList =  topRec[lmapBinName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  
  GP=F and info("[ENTER]: <%s:%s> Name-List(%s), Value-List(%s)",
                 MOD, meth, tostring(nameList), 
                 tostring(valueList));
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

  -- In LMAP with name-value pairs, all operations are done 
  -- based on key-comparison. So we will parse name-list 
  -- to do everything !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  local resultValue = nil;

  for i = 1, list.size( nameList ), 1 do
    GP=F and info("[DEBUG]: <%s:%s> Comparing Name-entry(%s) with Name-list(%s)",
                   MOD, meth, tostring(newName), tostring(nameList[i]));

    -- a value that does not exist, will have a nil binList 
    -- so we'll skip this if-loop for it completely                  
    if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
      -- CHECK IF THE KEY/NAME IS PRESENT, transform function applicable only to value  
      resultValue = unTransformComplexCompare(lmapCtrlInfo, nil, nameList[i], newName);
      if resultValue ~= nil then

        GP=F and info("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));

	-- Return point for FV_DELETE and FV_SCAN, what should we return here ?
        -- if its a search, the result should return the pair in the name-value pair
	-- if its a deleted, the result should be just the return code and the 
        -- result-list will be nil !!!!!!!!!!!!!!!!!!!!

        if( flag == FV_DELETE ) then
          local newString = nameList[i]..":"..valueList[i];  
          list.append( resultList, newString );
          nameList[i] = FV_EMPTY; -- the name-entry is NO MORE
          valueList[i] = FV_EMPTY; -- the value-entry is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = propMap[PM_ItemCount];
          propMap[PM_ItemCount] = itemCount - 1;
          lMapList[1] = propMap; 
          lmapCtrlInfo[M_CompactNameList] = nameList; 
          lmapCtrlInfo[M_CompactValueList] = valueList;
          lMapList[2] = lmapCtrlInfo;
          topRec[lmapBinName] = lMapList; 
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
          resultValue = unTransformComplexCompare(lmapCtrlInfo, unTranform, valueList[i], valueList[i]);
          GP=F and info(" FV_SCAN resultValue: %s", tostring(resultValue));  
           
          local resultFiltered;
	  if filter ~= nil and fargs ~= nil then
         	resultFiltered = functionTable[filter]( resultValue, fargs );
    	  else
      		resultFiltered = resultValue;
    	  end
          local newString = nameList[i]..":"..resultFiltered; 
          list.append( resultList, newString );
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
    GP=F and info("[EXIT]: <%s:%s> Inserting(%s)",
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
local function scanList( topRec, lmapBinName, resultList, newName, newValue, 
       flag, filter, fargs )
  local meth = "scanList()";
  local lMapList =  topRec[lmapBinName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];

  GP=F and info(" !!!!!! scanList:  Key-Type: %s !!!!!", tostring(lmapCtrlInfo[M_KeyType]));
  if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
    return simpleScanList( topRec, lmapBinName, resultList, newName, newValue, 
       flag, filter, fargs ); 
  else
    return complexScanList( topRec, lmapBinName, resultList, newName, newValue, 
       flag, filter, fargs ); 
  end
end

-- ======================================================================
-- localInsert( topRec, lmapBinName, newName, newValue, 1 );
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- !!!!!!!! IN LMAP THIS IS CALLED ONLY IN SS_COMPACT MODE !!!!!!!!!
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) lmapBinName: The LMap control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, lmapBinName, newName, newValue, stats )
 
  local meth = "localInsert()";
    
  GP=F and info("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));
  
  local binName = lmapBinName; 
  
  local lMapList =  topRec[lmapBinName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  -- This binList would've been created in setupNewLmapBin()   
  -- local binList = lmapCtrlInfo[M_CompactList]; 
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  local insertResult = 0;
  
  if nameList == nil or valueList == nil then
    GP=F and info("[INTERNAL ERROR]:<%s:%s> binList is nil: binName(%s)",
                 MOD, meth, tostring( binName ) );
    error( ldte.ERR_INTERNAL );
  else
    GP=F and info("[INTERNAL DUMP]:<%s:%s> binList is NOT nil: binName(%s) List %s",
                 MOD, meth, tostring( binName ), tostring(binList) );
    -- Look for the value, and insert if it is not there.
    insertResult =
      scanList( topRec, lmapBinName, nil, newName, newValue, FV_INSERT, nil, nil );
  end
                
  -- update stats if appropriate.
  -- The following condition is true only for FV_INSERT returning a success

  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = propMap[PM_ItemCount];
    local totalCount = lmapCtrlInfo[M_TotalCount];
    propMap[PM_ItemCount] = itemCount + 1; -- number of valid items goes up
    lmapCtrlInfo[M_TotalCount] = totalCount + 1; -- Total number of items goes up
    topRec[binName] = lMapList;
    record.set_flags(topRec, binName, BF_LDT_BIN );--Must set every time
  end
 
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
    GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
  else
    warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
  end 
  GP=F and info("[EXIT]: <%s:%s>Storing Record() with New Value(%s): List(%s)",
                 MOD, meth, tostring( newValue ), tostring( binList ) );
  -- No need to return anything
end -- localInsert


-- ======================================================================
-- rehashSetToLmap( src, topRec, lmapBinName,  newName, newValue );
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
local function rehashSetToLmap( src, topRec, lmapBinName,  newName, newValue )
  local meth = "rehashSetToLmap()";
  GP=F and info("[ENTER]:<%s:%s> !!!! REHASH !!!! Name: %s Src %s, Top: %s, Ctrl: %s, Name: %s Val : %s", 
		 MOD, meth, tostring(lmapBinName),tostring(src),tostring(topRec),tostring(lmapCtrlInfo), tostring(newName), tostring(newValue));

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  -- If we are calling rehashSet, we probably have only one LSET list which we
  -- can access directly with name as all LMAP bins are yser-defined names. 
  local lMapList =  topRec[lmapBinName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  -- local singleBinList = lmapCtrlInfo[M_CompactList];
  if nameList == nil or valueList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(lmapBinName));
    error( ldte.ERR_INSERT );
  end
  
  -- Copy existing elements into temp list
  local listNameCopy = list.take(nameList, list.size( nameList ));
  local listValueCopy = list.take(valueList, list.size( valueList ));
  lmapCtrlInfo[M_StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
 
  -- create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  
  GP=F and info("[ENTER]:<%s:%s> Calling initializeLMapRegular ", MOD, meth );
  initializeLMapRegular(topRec, lmapBinName); 
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = lmapCtrlInfo[M_Modulo];
  for i = 0, (distrib - 1), 1 do
    -- empty list created during initializeLmap()
    list.append( lmapCtrlInfo[M_DigestList], 0 );
  
  end -- for each new bin
  
  -- take-in the new element whose insertion request has triggered the rehash. 
  
  GP=F and info("%s:%s Before calling the first subrec-insert Name-list %s, Value-list %s ", MOD, meth, tostring(listNameCopy), tostring(listValueCopy) );
  list.append(listNameCopy, newName);
  list.append(listValueCopy, newValue);
  
  -- Before calling code to rehash and create-subrecs, reset COMPACT mode settings: 
  lmapCtrlInfo[M_CompactNameList] = nil; 
  lmapCtrlInfo[M_CompactValueList] = nil; 
  propMap[PM_ItemCount] = 0;
  -- TotalCount is the count of all elements including deletions. Technically these are not getting deleted. so we'll reset
  -- TODO : Add TotalCount math to deletions  !!! 
  lmapCtrlInfo[M_TotalCount] = 0; 

  for i = 1, list.size(listNameCopy), 1 do
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
  	  lmapLdrSubRecInsert( src, topRec, lmapBinName, listNameCopy[i], listValueCopy[i] ); 
  end
 
  
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );
end -- rehashSetToLmap()


local function lmapInsertRegular( topRec, lmapBinName, newName,  newValue)

  local meth = "lmapInsertRegular()";
  
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local totalCount = lmapCtrlInfo[M_TotalCount];

  GP=F and info("!!!!!!: <%s:%s> ListMode : %s value %s ThreshHold : %s!!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ), tostring( newValue ), tostring(lmapCtrlInfo[M_ThreshHold]));
  
   -- we are now processing insertion for a new element and we notice that 
   -- we've reached threshold. Excellent ! 
   -- so now, lets go and do a rehash-first and also follow-up with an 
   -- insertion for the new element. 
   
  local src = createSubrecContext();
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT and
      totalCount == lmapCtrlInfo[M_ThreshHold]    
  then
    -- !!! Here we are switching from compact to regular mode !!!
    -- refer to lmap_design.lua for functional notes 
    GP=F and info("!!!!!!: <%s:%s> ListMode : %s !!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ));
    rehashSetToLmap( src, topRec, lmapBinName,  newName, newValue );
  else
      GP=F and info("!!!!!!: <%s:%s>  ListMode : %s Direct-call %s!!!!!!!! ", MOD, meth, tostring( lmapCtrlInfo[M_StoreState] ), tostring(newValue) );
      lmapLdrSubRecInsert( src, topRec, lmapBinName, newName, newValue); 
  end
   
  GP=F and info("[EXIT]: <%s:%s>", MOD, meth );
  
end

local function localLMapCreate( topRec, lmapBinName, createSpec )
  local meth = "localLMapCreate()";
  
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
  -- flag set to false because we need not check for ctrl bins. 
  validateLmapParams( topRec, lmapBinName, false );

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LMAP field to an
  -- existing record.
  if( topRec[lmapBinName] ~= nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> LMAP CONTROL BIN Already Exists",
                   MOD, meth );
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end
  
  GP=F and info("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
  local lMapList = initializeLMap( topRec, lmapBinName);
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  
  -- Set the type of this record to LDT (it might already be set)
  record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in some settings that override our defaults
  -- (createSpec) then apply them now.
  if createSpec ~= nil then 
    adjustLMapCtrlInfo( lmapCtrlInfo, createSpec );
    -- Changes to the map need to be re-appended to topRec  
    GP=F and info(" After adjust Threshold : %s ", tostring( lmapCtrlInfo[M_ThreshHold] ) );
    topRec[lmapBinName] = lMapList;
    record.set_flags(topRec, lmapBinName, BF_LDT_BIN );--Must set every time
    
    GP=F and info("[DEBUG]: <%s:%s> : LMAP Summary after adjustLMapCtrlInfo(%s)",
       MOD, meth , lmapSummaryString(lMapList));
  end

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and info("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
    rc = lmap_update_topdigest( topRec, lmapBinName ); 
  else
    GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;

end -- end of localLMapCreate

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
local function localLMapInsert( topRec, lmapBinName, newName, newValue, createSpec )
  local meth = "localLMapInsert()";
   
  GP=F and info("[ENTRY]: <%s:%s> Bin-Name: %s, name-value pair = %s:%s, spec:%s", MOD, meth, 
           tostring(lmapBinName), tostring(newName),tostring(newValue),tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateLmapParams( topRec, lmapBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[lmapBinName] == nil ) then
    warn("!!!!!!: <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    rc = localLMapCreate( topRec, lmapBinName, createSpec ); 

    if rc == 0 then 
       GP=F and info("localLMapCreate DONE: <%s:%s> Bin-Name: %s, name-value pair = %s:%s, spec:%s", MOD, meth, 
           tostring(lmapBinName), tostring(newName),tostring(newValue),tostring(createSpec) );
    else 
      warn("[ERROR]<%s:%s> lmap-creation failed", MOD, meth );
      error( ldte.ERR_INTERNAL );
    end 
  end
  
  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local lMapList = topRec[lmapBinName]; -- The main lmap
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
    localInsert( topRec, lmapBinName, newName, newValue, 1 );
  else
    lmapInsertRegular( topRec, lmapBinName, newName, newValue); 
  end
  
   -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and info("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
    rc = lmap_update_topdigest( topRec, lmapBinName ); 
  else
    GP=F and info("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and info("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc
  
end -- function localLMapInsert()

-- ======================================================================
-- ldrDeleteList( topLdrChunk, lMapList, listIndex,  insertList, filter, fargs )
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
-- (*) entryList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================

local function ldrDeleteList(topRec, lmapBinName, ldrChunkRec, listIndex, entryList, filter, fargs)
  local meth = "ldrDeleteList()";

  GP=F and info("[ENTER]: <%s:%s> Index(%d) Search-List(%s)",
    MOD, meth, listIndex, tostring( entryList ) );

  local lMapList = topRec[lmapBinName]; 
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
  local self_digest = record.digest( ldrChunkRec ); 

  -- These 2 get assigned in lmapLdrListChunkCreate() to point to the ctrl-map. 
  local ldrNameList =  ldrChunkRec[LDR_NLIST_BIN];
  local ldrValueList = ldrChunkRec[LDR_VLIST_BIN];
  local ldrMap = ldrChunkRec[LDR_CTRL_BIN];

  if ldrNameList == nil then 
    -- Nothing to be deleted in this subrec
    GP=F and info("[ENTER]: <%s:%s> Nothing to be deleted in this subrec !!",
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
  local totalCount = lmapCtrlInfo[M_TotalCount];
  propMap[PM_ItemCount] = itemCount - num_deleted; -- number of valid items goes down
  lmapCtrlInfo[M_TotalCount] = totalCount - num_deleted; -- Total number of items goes down 
  
  GP=F and info(" Delete : Num-deleted :%s Mapcount %s", tostring(num_deleted), tostring(propMap[PM_ItemCount])); 
 
  -- Now go and fix the digest-list IF NEEDED 
  -- refer to lmap_design.lua to determine what needs to be done here.
  -- we deleted the one and only (or last) item in the LDR list. 
  if totalListSize == totalItemsToDelete and list.size( ldrChunkRec[LDR_NLIST_BIN] ) == 0 then
    GP=F and info("[DEBUG] !!!!!!!!! Entire LDR list getting Deleted !!!!!!");
    local digestlist = lmapCtrlInfo[M_DigestList]; 
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
   lmapCtrlInfo[M_DigestList] = digestlist; 
   
 end -- end of if check for digestlist reset 
   
  topRec[lmapBinName] = lMapList;
  record.set_flags(topRec, lmapBinName, BF_LDT_BIN );--Must set every time
  rc = aerospike:update( topRec );
  if( rc == nil or rc == 0 ) then
     GP=F and info("[EXIT]: <%s:%s>", MOD, meth );      
  else
     warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%s)",MOD,meth,tostring(rc));
     error( ldte.ERR_SUBREC_UPDATE );
  end 
   
 return num_deleted;
end -- ldrDeleteList()

-- ==========================================================================

local function localLMapDelete( topRec, lmapBinName, searchValue,
                          filter, fargs )
  local meth = "localLMapDelete()";
                            
  GP=F and info("[ENTER]:<%s:%s> Bin-Name(%s) Delete-Value(%s) ",
        MOD, meth, tostring(lmapBinName), tostring(searchValue));      
         
  local resultList = list(); -- add results to this list.
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateLmapParams( topRec, lmapBinName, true );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec[lmapBinName] == nil ) then
    GP=F and info("[ERROR EXIT]: <%s:%s> LMapCtrlBin does not Exist",
                   MOD, meth );
     error( ldte.ERR_INTERNAL );
  end
  
    -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local index = 0; 
  
  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then 
	  local binList = lmapCtrlInfo[M_CompactList];
	  -- Fow now, scanList() will only NULL out the element in a list, but will
	  -- not collapse it.  Later, if we see that there are a LOT of nil entries,
	  -- we can RESET the set and remove all of the "gas".
	  
          rc = scanList(topRec, lmapBinName, resultList, searchValue, nil, FV_DELETE, filter, fargs);
	  -- If we found something, then we need to update the bin and the record.
	  if rc == 0 and list.size( resultList ) > 0 then
	    -- We found something -- and marked it nil -- so update the record
	    -- lmapCtrlInfo[M_CompactList] = binList;
	    rc = aerospike:update( topRec );
	    if( rc < 0 ) then
              error( ldte.ERR_DELETE );
	    end
	  elseif rc == 0 and list.size( resultList ) == 0 then 
		-- This item does not exist
		-- return a not-found error  
              error( ldte.ERR_DELETE );
	  end
	  
	  return rc;
  else
  	-- we are in regular mode !!! 
  	GP=F and info("[ENTER]:<%s:%s> Doing LMAP delete in regular mode ", MOD, meth );
  	
        local digestlist = lmapCtrlInfo[M_DigestList]; 
  	
  	GP=F and info(" DigestList %s Size: %s", tostring(digestlist), tostring(list.size(digestlist)));
  	
  	-- First obtain the hash for this entry
  	local digest_bin = computeSetBin( searchValue, lmapCtrlInfo );  	
	
        -- sanity check for absent entries 
	if  digestlist[digest_bin] == 0 then 
	  warn("[ERROR]: <%s:%s>: Digest-List index is empty for this value %s ", MOD, meth, tostring(searchValue));
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
    
       GP=F and info("!!!!!!!!! Find match digest value: %s", tostring(delChunkDigest));
    
       -- HACK : TODO : Fix this number to list conversion  
       local entryList = list(); 
       list.append(entryList, searchValue); 
  
       local totalEntryCount = list.size( entryList );
       GP=F and info("[DEBUG]: <%s:%s> Calling ldrDeleteList: List(%s) Count: %s",
                       MOD, meth, tostring( entryList ), tostring(totalEntryCount));
  
       -- The magical function that is going to fix our deletion :)
       local num_deleted = ldrDeleteList(topRec, lmapBinName, IndexLdrChunk, 1, entryList, filter, fargs);
    
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
    MOD, meth, ldrChunkSummary( IndexLdrChunk ), tostring(lmapCtrlInfo[M_DigestList]));
    return 0; 	  

  end -- end of regular mode deleteion 

end -- localLMapDelete()

local function ldrSearchList(topRec, lmapBinName, resultList, ldrChunkRec, listIndex, entryList, filter, fargs)

  local meth = "ldrSearchList()";
  GP=F and info("[ENTER]: <%s:%s> Index(%d) List(%s)",
           MOD, meth, listIndex, tostring( entryList ) );

  local lMapList = topRec[lmapBinName]; 
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2];
  local binName = propMap[PM_BinName];
  local self_digest = record.digest( ldrChunkRec ); 

  -- These 2 get assigned in lmapLdrListChunkCreate() to point to the ctrl-map. 
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
        if filter ~= nil and fargs ~= nil then
          resultFiltered = functionTable[filter]( ldrValueList[i], fargs );
        else
      	  resultFiltered = ldrValueList[i];
        end
        local newString = ldrNameList[i]..":"..resultFiltered; 
        list.append( resultList, newString );
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
	  if filter ~= nil and fargs ~= nil then
            resultFiltered = functionTable[filter]( ldrValueList[i], fargs );
    	  else
      	    resultFiltered = ldrValueList[i];
    	  end
          local newString = ldrNameList[i]..":"..resultFiltered; 
          list.append( resultList, newString );
        end
    end 
    end -- for each remaining entry
    -- Nothing to be stored back in the LDR ctrl map 
  end
  
  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and info("!!!![DEBUG]:<%s:%s>:Result List after Search OP %s!!!!!!!!!!",
       MOD, meth, tostring( resultList ) );
       
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
local function simpleScanListAll(topRec, binName, resultList, filter, fargs) 

  local meth = "simpleScanListAll()";
  GP=F and info("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local lMapList =  topRec[binName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lmapCtrlInfo[M_Transform] ~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
  end
   
  GP=F and info(" Parsing through :%s ", tostring(binName))

  if nameList ~= nil then
    for i = 1, list.size( nameList ), 1 do
      if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
        retValue = valueList[i]; 
	if unTransform ~= nil then
	  retValue = unTransform( valueList[i] );
	end

        local resultFiltered;

	if filter ~= nil and fargs ~= nil then
 	  resultFiltered = functionTable[filter]( retValue, fargs );
	else
      	  resultFiltered = retValue;
    	end
        local newString = nameList[i]..":"..resultFiltered; 
	list.append( resultList, newString );
	listCount = listCount + 1; 
      end -- end if not null and not empty
    end -- end for each item in the list
  end -- end of topRec null check 

  GP=F and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
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
local function simpleDumpListAll(topRec, resultList, lMapList, binName, filter, fargs) 

  local meth = "simpleDumpListAll()";
  GP=F and info("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lmapCtrlInfo[M_Transform] ~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
  end
   
    GP=F and info(" Parsing through :%s ", tostring(binName))

	if lmapCtrlInfo[M_CompactList] ~= nil then
		local objList = lmapCtrlInfo[M_CompactList];
	        list.append( resultList, "\n" );
		for i = 1, list.size( objList ), 1 do
                        local indexentry = "INDEX:" .. tostring(i); 
			list.append( resultList, indexentry );
			if objList[i] ~= nil and objList[i] ~= FV_EMPTY then
				retValue = objList[i]; 
				if unTransform ~= nil then
					retValue = unTransform( objList[i] );
				end

        			local resultFiltered;

				if filter ~= nil and fargs ~= nil then
        				resultFiltered = functionTable[func]( retValue, fargs );
			    	else
      					resultFiltered = retValue;
    				end

			        list.append( resultList, resultFiltered );
				listCount = listCount + 1;
                        else 
			        list.append( resultList, "EMPTY ITEM" );
			end -- end if not null and not empty
			list.append( resultList, "\n" );
		end -- end for each item in the list
	end -- end of topRec null check 

  GP=F and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)

  return 0; 
end -- simpleDumpListAll

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List, append all the items in the list to result.
--
-- TODO :  
-- This is COMPLEX SCAN, currently an exact copy of the simpleScanListAll().
-- I need to first write an unTransformComplexCompare() which involves
-- using the compare function, to write a new complexScanListAll()  
--
-- Parms:
-- (*) binList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanListAll(topRec, binName, resultList, filter, fargs) 
  local meth = "complexScanListAll()";
  GP=F and info("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)

  local lMapList =  topRec[binName];
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local nameList = lmapCtrlInfo[M_CompactNameList]; 
  local valueList = lmapCtrlInfo[M_CompactValueList]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;

  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  if lmapCtrlInfo[M_Transform] ~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
  end
   
  GP=F and info(" Parsing through :%s ", tostring(binName))

  if nameList ~= nil then
    for i = 1, list.size( nameList ), 1 do
      if nameList[i] ~= nil and nameList[i] ~= FV_EMPTY then
        retValue = valueList[i]; 
	if unTransform ~= nil then
	  retValue = unTransform( valueList[i] );
	end

        local resultFiltered;

	if filter ~= nil and fargs ~= nil then
 	  resultFiltered = functionTable[filter]( retValue, fargs );
	else
      	  resultFiltered = retValue;
    	end
        local newString = nameList[i]..":"..resultFiltered; 
	list.append( resultList, newString );
	listCount = listCount + 1; 
      end -- end if not null and not empty
    end -- end for each item in the list
  end -- end of topRec null check 

  GP=F and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
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
-- (*) binList: the list of values from the record
-- Return: resultlist 
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexDumpListAll(topRec, resultList, lMapList, binName, filter, fargs) 
  local meth = "complexDumpListAll()";
  GP=F and info("[ENTER]: <%s:%s> Appending all the elements of List ",
                 MOD, meth)
                 
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local listCount = 0;
  local transform = nil;
  local unTransform = nil;
  local retValue = nil;
  
  if lmapCtrlInfo[M_Transform] ~= nil then
    transform = functionTable[lmapCtrlInfo[M_Transform]];
  end

  if lmapCtrlInfo[M_UnTransform] ~= nil then
    unTransform = functionTable[lmapCtrlInfo[M_UnTransform]];
  end

    GP=F and info(" Parsing through :%s ", tostring(binName))
	local binList = lmapCtrlInfo[M_CompactList];
	local resultValue = nil;
    if topRec[binName] ~= nil then
	        list.append( resultList, "\n" );
		for i = 1, list.size( binList ), 1 do
                        local indexentry = "INDEX:" .. tostring(i); 
			list.append( resultList, indexentry );
			if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
				retValue = binList[i]; 
				if unTransform ~= nil then
					retValue = unTransform( binList[i] );
				end
        			local resultFiltered;

				if filter ~= nil and fargs ~= nil then
        				resultFiltered = functionTable[func]( retValue, fargs );
			    	else
      					resultFiltered = retValue;
    				end

			        list.append( resultList, resultFiltered );
				listCount = listCount + 1; 
                        else 
			        list.append( resultList, "EMPTY ITEM" );
			end -- end if not null and not empty
			list.append( resultList, "\n" );
  		end -- end for each item in the list
    end -- end of topRec null check 

 GP=F and info("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, listCount)

  return 0; 
end -- complexDumpListAll

local function localLMapSearchAll(topRec,lmapBinName,resultList,filter,fargs)
  
  local meth = "localLMapSearchAll()";
  rc = 0; -- start out OK.
  GP=F and info("[ENTER]: <%s:%s> Bin-Name: %s Search for Value(%s)",
                 MOD, meth, tostring(lmapBinName), tostring( searchValue ) );
                 
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local binName = lmapBinName;
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then 
    -- Find the appropriate bin for the Search value
    GP=F and info(" !!!!!! Compact Mode LMAP Search Key-Type: %s !!!!!", tostring(lmapCtrlInfo[M_KeyType]));
    local binList = lmapCtrlInfo[M_CompactList];
	  
    if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
      rc = simpleScanListAll(topRec, binName, resultList, filter, fargs) 
    else
      rc = complexScanListAll(topRec, binName, resultList, filter, fargs)
    end
	
    GP=F and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
    -- HACK : TODO : Fix this number to list conversion  
    local digestlist = lmapCtrlInfo[M_DigestList];
    local src = createSubrecContext();
	
    -- for each digest in the digest-list, open that subrec, send it to our 
    -- routine, then get the list-back and keep appending and building the
    -- final resultList. 
	  
    for i = 1, list.size( digestlist ), 1 do
      if digestlist[i] ~= 0 then 
        local stringDigest = tostring( digestlist[i] );
        local IndexLdrChunk = openSubrec( src, topRec, stringDigest );
        GP=F and info("[DEBUG]: <%s:%s> Calling ldrSearchList: List(%s)",
			           MOD, meth, tostring( entryList ));
			  
        -- temporary list having result per digest-entry LDR 
        local ldrlist = list(); 
        local entryList  = list(); 
        -- The magical function that is going to fix our deletion :)
        rc = ldrSearchList(topRec, lmapBinName, ldrlist, IndexLdrChunk, 0, entryList, filter, fargs );
        if( rc == nil or rc == 0 ) then
       	  GP=F and info("AllSearch returned SUCCESS %s", tostring(ldrlist));
	  for j = 1, list.size(ldrlist), 1 do 
 	    -- no need to filter here, results are already filtered in-routine
            list.append( resultList, ldrlist[j] );
    	  end
         else
      	   warn("%s: %s, Search returned FAILURE", mod, meth);
	 end -- end of if-else check 
         rc = closeSubrec( src, stringDigest )
       end -- end of digest-list if check  
     end -- end of digest-list for loop 
     -- Close ALL of the subrecs that might have been opened
     rc = closeAllSubrecs( src );
  end -- end of else 
  	  
  return resultList;
end -- end of localLMapSearchAll
 
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Map Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return the item if the item exists in the set.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
local function localLMapSearch(topRec, lmapBinName, searchValue, resultList,
                filter, fargs)

  local meth = "localLMapSearch()";
  rc = 0; -- start out OK.
  GP=F and info("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );
                 
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local binName = lmapBinName;
  local resultList = list(); -- add results to this list.
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then 
	  -- Find the appropriate bin for the Search value
	  GP=F and info(" !!!!!! Compact Mode LMAP Search !!!!!");
          local binList = lmapCtrlInfo[M_CompactList];
          GP=F and info("Value of List : %s Ctrl-Data : %s", tostring(binList), tostring(lmapCtrlInfo));
	  -- here binList is the target-list for a search. 
       
	  rc = 
	    scanList(topRec, lmapBinName, resultList, searchValue, nil, FV_SCAN, filter, fargs);
	
	  GP=F and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(resultList));
	                 
  else
  	  GP=F and info(" !!!!!! Regular Mode LMAP Search !!!!!");
          local digestlist = lmapCtrlInfo[M_DigestList]; 
  	
  	  GP=F and info(" DigestList %s Size: %s", tostring(digestlist), tostring(list.size(digestlist)));
  	
  	  -- First obtain the hash for this entry
  	  local digest_bin = computeSetBin( searchValue, lmapCtrlInfo );  	
  	  
      -- local digest_bin = index; 
	  -- Dont do an open_subrec, call our local function to handle this
	  
	  -- sanity check for absent entries 
	  if  digestlist[digest_bin] == 0 then 
	      warn("[ERROR]: <%s:%s>: Digest-List index is empty for this value %s ", MOD, meth, tostring(searchValue));
	  	  return resultList;
	  end 
	  
	  local stringDigest = tostring( digestlist[digest_bin] );
	  local src = createSubrecContext();
	
      -- GP=F and info(" Digest index : %d string-value: %s", digest_bin, stringdigest );
	
      local IndexLdrChunk = openSubrec( src, topRec, stringDigest );
   	
	  if IndexLdrChunk == nil then
 	  	-- sanity check 
        warn("[ERROR]: <%s:%s>: IndexLdrChunk nil or empty", MOD, meth);
        error( ldte.ERR_SUBREC_OPEN );
      end
    
      local delChunkDigest = record.digest( IndexLdrChunk );
    
      GP=F and info("!!!!!!!!! Find match digest value: %s", tostring(delChunkDigest));
    
	  -- HACK : TODO : Fix this number to list conversion  
      local entryList = list(); 
      list.append(entryList, searchValue); 
  
      local totalEntryCount = list.size( entryList );
      GP=F and info("[DEBUG]: <%s:%s> Calling ldrSearchList: List(%s)",
           MOD, meth, tostring( entryList ));
  
      -- The magical function that is going to fix our search :)
      rc = ldrSearchList(topRec, lmapBinName, resultList, IndexLdrChunk, 1,entryList, filter, fargs );
  	
  	  if( rc == nil or rc == 0 ) then
  	  	 GP=F and info("Search returned SUCCESS");
                -- Close ALL of the subrecs that might have been opened
                rc = closeAllSubrecs( src );
  	  else
  	  	 GP=F and info("Search returned FAILURE");
  	  end
  	  
  	  -- No need to update toprec, subrec or any such stats. Just return resultList
  	  	
  end -- end of regular mode else part

  return resultList;
end -- function localLMapSearch()

-- ========================================================================
-- ldt_remove() -- Remove the LDT entirely from the record.
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
local function ldt_remove( topRec, lmapBinName )
  local meth = "ldt_remove()";

  GP=F and info("[ENTER]: <%s:%s> binName(%s)",
    MOD, meth, tostring(lmapBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  -- Extract the property map and lso control map from the lso bin list.

  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  
  GP=F and info("[STATUS]<%s:%s> propMap(%s) LDT Summary(%s)", MOD, meth,
    tostring( propMap ), lmapSummaryString( lMapList ));

  if lmapCtrlInfo[M_StoreState] ~= SS_COMPACT then 
  	-- Get the ESR and delete it.
	  local esrDigest = propMap[PM_EsrDigest];
          local esrDigestString = tostring(esrDigest);
	  local esrRec = aerospike:open_subrec( topRec, esrDigestString );
	  GP=F and info("[STATUS]<%s:%s> About to Call Aerospike REMOVE", MOD, meth );
	  rc = aerospike:remove_subrec( esrRec );
	  if( rc == nil or rc == 0 ) then
   	    GP=F and info("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
  	  else
    	    warn("[ESR DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, binName);
    	    error( ldte.ERR_SUBREC_DELETE );
          end
  end 

  -- Mark the enitre control-info structure nil 
  topRec[lmapBinName] = nil;

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
  GP=F and info("[EXIT]: <%s:%s> : Done.  RC(%s)", MOD, meth, tostring(rc));

  return rc;
end -- ldt_remove()

local function localLMapWalkThru(resultList,topRec,lmapBinName,filter,fargs)
  
  local meth = "localLMapWalkThru()";
  rc = 0; -- start out OK.
  GP=F and info("[ENTER]: <%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchValue ) );
                 
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local binName = lmapBinName;
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  if lmapCtrlInfo[M_StoreState] == SS_COMPACT then 
	  -- Find the appropriate bin for the Search value
	  GP=F and info(" !!!!!! Compact Mode LMAP Search !!!!!");
	  local binList = lmapCtrlInfo[M_CompactList];
          list.append( resultList, " =========== LMAP WALK-THRU COMPACT MODE \n ================" );
	  
	  if lmapCtrlInfo[M_KeyType] == KT_ATOMIC then
		rc = simpleDumpListAll(topRec, resultList, lMapList, binName, filter, fargs) 
	  else
		rc = complexDumpListAll(topRec, resultList, lMapList, binName, filter, fargs)
	  end
	
	  GP=F and info("[EXIT]: <%s:%s>: Search Returns (%s)",
	                 MOD, meth, tostring(result));
  else -- regular searchAll
	  -- HACK : TODO : Fix this number to list conversion  
	  local digestlist = lmapCtrlInfo[M_DigestList];
	  local src = createSubrecContext();
	
	  -- for each digest in the digest-list, open that subrec, send it to our 
	  -- routine, then get the list-back and keep appending and building the
	  -- final resultList. 
	   
          list.append( resultList, "\n =========== LMAP WALK-THRU REGULAR MODE \n ================" );
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
	          rc = ldrSearchList(topRec, lmapBinName, ldrlist, IndexLdrChunk, 0, entryList, filter, fargs );
		  if( rc == nil or rc == 0 ) then
		  	GP=F and info("AllSearch returned SUCCESS %s", tostring(ldrlist));
        	        list.append( resultList, "LIST-ENTRIES:" );
			for j = 1, list.size(ldrlist), 1 do 
 			  -- no need to filter here, results are already filtered in-routine
        		  list.append( resultList, ldrlist[j] );
    		        end
		  end -- end of if-rc check 
                  rc = closeSubrec( src, stringDigest )
              else -- if digest-list is empty
      		 list.append( resultList, "EMPTY ITEM")
	      end -- end of digest-list if check  
              list.append( resultList, "\n" );
	  end -- end of digest-list for loop 
          list.append( resultList, "\n =========== END :  LMAP WALK-THRU REGULAR MODE \n ================" );
          -- Close ALL of the subrecs that might have been opened
          rc = closeAllSubrecs( src );
  end -- end of else 

  return resultList;
end -- end of localLMapWalkThru
 
local function localLMapInsertAll( topRec, binName, nameValMap, createSpec )
  local meth = "localLMapInsertAll()";
  for name, value in map.pairs( nameValMap ) do
    GP=F and info("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s) TYPE : %s",
        MOD, meth, tostring( name ), tostring( value ), type(value));
    rc = localLMapInsert( topRec, binName, name, value, createSpec )
    GP=F and info("[DEBUG]: <%s:%s> : lmap insertion for %s %s RC(%d)", MOD, meth, tostring(name), tostring(value), rc );
    return rc; 
  end 
end

-- =======================================================================================================================
-- OLD EXTERNAL FUNCTIONS
-- =======================================================================================================================

-- ======================================================================
-- lmap_search() -- with and without filter
-- ======================================================================
function lmap_search( topRec, lmapBinName, searchName )
  GP=F and info("\n\n >>>>>>>>> API[ lmap_search ] <<<<<<<<<< \n\n");
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue

  validateLmapParams( topRec, lmapBinName, true );
  if( searchName == nil ) then
    -- if no search value, use the faster SCAN (searchALL)
    return localLMapSearchAll(topRec,lmapBinName,resultList,nil,nil)
  else
    return localLMapSearch(topRec,lmapBinName,searchName,resultList,nil,nil)
  end
end -- lmap_search()

-- ======================================================================

function
lmap_search_then_filter( topRec, lmapBinName, searchName, filter, fargs )
  GP=F and info("\n\n >>>>>>>>> API[ lmap_search_then_filter ] <<<<<<<<<< \n\n");
  resultList = list();
  -- if we dont have a searchValue, get all the list elements.
  -- Note that this means an empty searchValue which is not 
  -- the same as a nil or a NULL searchValue

  validateLmapParams( topRec, lmapBinName, true );
  if( searchName == nil ) then
    -- if no search value, use the faster SCAN (searchALL)
    return localLMapSearchAll(topRec,lmapBinName,resultList,filter,fargs)
  else
    return localLMapSearch(topRec,lmapBinName,searchName,resultList,filter, fargs)
  end
end -- lmap_search_then_filter()

-- ========================================================================
-- lmap_remove() -- Remove the LDT entirely from the record.
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
function lmap_remove( topRec, lmapBinName )
  GP=F and info("\n\n >>>>>>>>> API[ LMAP REMOVE ] <<<<<<<<<< \n\n");
  return ldt_remove( topRec, lmapBinName );
end

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
function lmap_scan( topRec, lmapBinName )
  local meth = "lmap_scan()";
  GP=F and info("[ENTER]<%s:%s> LLIST BIN(%s)",
    MOD, meth, tostring(lmapBinName) );

  validateLmapParams( topRec, lmapBinName, true );
  resultList = list();
  GP=F and info("\n\n  >>>>>>>> API[ SCAN ] <<<<<<<<<<<<<<<<<< \n");

  return localLMapSearchAll(topRec,lmapBinName,resultList,nil,nil);
end -- end llist_scan()

-- ========================================================================
-- lmap_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function lmap_size( topRec, lmapBinName )
  local meth = "lmap_size()";

  GP=F and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(lmapBinName));

  GP=F and info("\n\n >>>>>>>>> API[ LMAP SIZE ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  local lMapList = topRec[lmapBinName]; -- The main lmap
  local propMap = lMapList[1]; 
  local lmapCtrlInfo = lMapList[2]; 
  local itemCount = propMap[PM_ItemCount];

  GP=F and info("[EXIT]: <%s:%s> : SIZE(%d)", MOD, meth, itemCount );
  GP=F and info(" !!!!!! lmap_size: Search Key-Type: %s !!!!!", tostring(lmapCtrlInfo[M_KeyType]));

  return itemCount;
end -- function lmap_size()


-- ========================================================================
-- lmap_config() -- return the config settings
-- ========================================================================
function lmap_config( topRec, lmapBinName )
  local meth = "lmap_config()";

  GP=F and info("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(lmapBinName));

  GP=F and info("\n\n >>>>>>>>> API[ LMAP CONFIG ] <<<<<<<<<< \n\n");

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  local lMapList = topRec[lmapBinName]; -- The main lmap
  local config = lmapSummaryString(lMapList); 

  GP=F and info("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config) );
  
  return config;
end -- function lmap_config()

-- ======================================================================
-- lmap_delete() -- with and without filter
-- Return resultList
-- (*) If successful: return deleted items (list.size( resultList ) > 0)
-- (*) If error: resultList will be an empty list.
-- ======================================================================
function lmap_delete( topRec, lmapBinName, searchName )
  return localLMapDelete(topRec, lmapBinName, searchName, nil, nil )
end -- lmap_delete()

function lmap_delete_then_filter( topRec, lmapBinName, searchName,
                                  filter, fargs )
  return localLMapDelete( topRec, lmapBinName, searchName,
                          filter, fargs )
end -- lmap_delete_then_filter()

-- ======================================================================
-- lmap_insert() -- with and without create
-- ======================================================================
function lmap_insert( topRec, lmapBinName, newName, newValue )
  return localLMapInsert( topRec, lmapBinName, newName, newValue, nil );
end -- lmap_insert()

function lmap_create_and_insert( topRec, lmapBinName, newName, newValue, createSpec )
  return localLMapInsert( topRec, lmapBinName, newValue, createSpec )
end -- lmap_create_and_insert()

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
  
  warn("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(lmapBinName), tostring(createSpec) );

  local rc = localLMapCreate( topRec, lmapBinName, createSpec );
  return rc; 
 
end -- lmap_create()

-- ========================================================================
-- lmap_dump()
-- ========================================================================
-- Dump the full contents of the Large Map, with Separate Hash Groups
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function lmap_dump( topRec, binName )
  local meth = "lmap_dump()";
  GP=F and info("[ENTER]<%s:%s> ", MOD, meth);

  local lmapBinName = binName; 

  GP=F and info("[ENTER]<%s:%s> LLIST BIN(%s)",
    MOD, meth, tostring(lmapBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateLmapParams( topRec, lmapBinName, true );

  resultList = list();
  GP=F and info("\n\n  >>>>>>>> API[ DUMP ] <<<<<<<<<<<<<<<<<< \n");

  localLMapWalkThru(resultList,topRec,lmapBinName,nil,nil);

  for i = 1, list.size( resultList ), 1 do
     info(tostring(resultList[i]));
  end 

  -- Another key difference between dump and scan : 
  -- dump prints things in the logs and returns a 0
  -- scan returns the list to the client/caller 

  local ret = " \n Lmap bin contents dumped to server-logs \n"; 
  return ret; 
end -- lmap_dump();

-- ======================================================================
-- lmap_insert_all() -- with and without create
-- ======================================================================
function lmap_insert_all( topRec, binName, NameValMap, createSpec )
  return localLMapInsertAll( topRec, binName, NameValMap, createSpec )
end

function lmap_create_and_insert_all( topRec, binName, NameValMap, createSpec )
  return localLMapInsertAll( topRec, binName, NameValMap, createSpec )
end
