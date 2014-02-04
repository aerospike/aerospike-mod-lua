-- Large Data Type (LDT) Common Functions
-- ldt_common.lua:  July 24, 2013 (Happy Birthday Jamie!!)
--
module('ldt_common', package.as_ldt)

-- Module Marker: Keep this in sync with the stated version
local MOD="ldt_common_2013_07_24.a"; -- the module name used for tracing

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

-- ======================================================================
-- LDT COMMON TODO LIST:
-- Future: Short, Medium and Long Term
-- Priority: High, Medium and Low
-- Difficulty: High, Medium and Low
-- ======================================================================
-- ACTIVITIES LIST:
-- (*) IN-PROGRESS: (July 23, 2012)
--   + Switch to ldt_common.lua for the newly common functions.
--     (*) Subrec methods
--     (*) List Processing Methods
--     (*) Common Property Map Methods
--
-- TODO:
--
-- ======================================================================
-- DONE:
--
-- ======================================================================
-- Methods defined in this module:
-- LIST METHODS
-- listAppend( baseList, additionList )
-- SUBREC CONTEXT METHODS
-- ======================================================================
-- Aerospike Record Calls:
-- status = aerospike:remove( record )
--
-- Aerospike SubRecord Calls:
-- newRec = aerospike:create_subrec( topRec )
-- newRec = aerospike:open_subrec( topRec, childRecDigest)
-- status = aerospike:update_subrec( childRec )
-- status = aerospike:close_subrec( childRec )
-- digest = record.digest( childRec )
-- status = record.set_type( topRec, recType )
-- status = record.set_flags( topRec, binName, binFlags )
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

-- Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error
--
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

-- All LDT subrecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN   = "SR_PROP_BIN";

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

-- =============================
-- Begin SubRecord Function Area
-- =============================
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, will employ the "subrecContext" object, which
-- will hold all of the subrecords that were opened during processing. 
-- Note that some operations can potentially involve many subrec
-- operations -- and can also potentially revisit pages.
--
-- SubRecContext Design:
-- The Subrec Object is a list of two maps.
-- The first map is always the Record map, where a digestString maps to
-- a subrec pointer.
-- The second map is the "DirtyMap", where a digestString maps a boolean
-- that reveals "True" or "False".
--
-- It's not clear if we should have a list of two maps, or a map of maps,
-- where each digest==>RecMap, and the rec map has the rec ptr and dirty
-- status.  We'll have to see if one has a higher management cost than the
-- other (it's a Chris question);
-- TODO: @TOBY: Ask Chris the Map Management Question.
--
-- At the end of an outer call, we will iterate thru the subrec
-- context and close all open subrecords, although, given the design of
-- the subrec mechanism, closing may not actually be needed.
-- ======================================================================
function createSubrecContext()
  local meth = "createSubrecContext()";
  GP=F and trace("[ENTER]<%s:%s>", MOD, meth );

  -- We need to track BOTH the Open Records and their Dirty State.
  -- Do this with a LIST of maps:
  -- recMap   = srcList[1]
  -- dirtyMap = srcList[2]

  -- Create the two maps and the SRC LIST.
  local srcList = list();
  local recMap = map();
  local dirtyMap = map();
  recMap.ItemCount = 0;
  list.append( srcList, recMap ); -- recMap
  list.append( srcList, dirtyMap ); -- dirtyMap

  GP=F and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return srcList;
end -- createSubrecContext()

-- ======================================================================
-- createSubrec( srcList, topRec )
-- ======================================================================
-- Create a subrec and add it to the subrec context, then return the new
-- subrec to the caller.
-- ======================================================================
function createSubrec( srcList, topRec )
  local meth = "createSubrec()";
  GP=F and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring( srcList));

  if( srcList == nil ) then
    error("[BAD SUB REC CONTEXT] src is nil");
  end

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  local digest = record.digest( subrec );
  local digestString = tostring( digest );
  recMap[digestString] = subrec;

  local itemCount = recMap.ItemCount;
  recMap.ItemCount = itemCount + 1;

  GP=F and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- Given an already opened subrec (probably one that was recently created),
-- add it to the subrec context.
-- ======================================================================
function addSubrecToContext( srcList, subrec )
  local meth = "addSubrecContext()";
  GP=F and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring( srcList));

  if( srcList == nil ) then
    error("[BAD SUB REC CONTEXT] src is nil");
  end

  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  local digest = record.digest( subrec );
  local digestString = tostring( digest );
  recMap[digestString] = subrec;

  local itemCount = recMap.ItemCount;
  recMap.ItemCount = itemCount + 1;

  GP=F and trace("[EXIT]: <%s:%s> : SRC(%s)", MOD, meth, tostring(srcList));
  return 0;
end -- addSubrecToContext()

-- ======================================================================
-- openSubrec()
-- ======================================================================
function openSubrec( srcList, topRec, digestString )
  local meth = "openSubrec()";
  GP=F and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
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
      error("[SUBREC OPEN LIMIT]: Exceeded Open Subrec Limit");
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
      error("[SUBREC OPEN FAILURE]: Couldn't open Subrec");
    end
  else
    GP=F and trace("[FOUND REC]<%s:%s>Rec(%s)", MOD, meth, tostring(subrec));
  end

  GP=F and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s)",
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
function closeSubrec( srcList, digestString )
  local meth = "closeSubrec()";
  GP=F and trace("[ENTER]<%s:%s> DigestStr(%s) SRC(%s)",
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
    error("[INTERNAL ERROR]: Rec not found ");
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

  GP=F and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- closeSubrec()


-- ======================================================================
-- updateSubrec()
-- ======================================================================
-- Update the subrecord -- and then mark it dirty.
-- ======================================================================
function updateSubrec( srcList, subrec, digest )
  local meth = "updateSubrec()";
  GP=F and trace("[ENTER]<%s:%s> TopRec(%s) DigestStr(%s) SRC(%s)",
    MOD, meth, tostring(topRec), digestString, tostring(srcList));

  local recMap = srcList[1];
  local dirtyMap = srcList[2];
  local rc = 0;

  if( digest == nil or digest == 0 ) then
    digest = record.digest( subrec );
  end
  local digestString = tostring( digest );

  aerospike:update_subrec( subrec );
  dirtyMap[digestString] = true;

  GP=F and trace("[EXIT]<%s:%s>Rec(%s) Dig(%s) rc(%s)",
    MOD, meth, tostring(subrec), digestString, tostring(rc));
  return rc;
end -- updateSubrec()

-- ======================================================================
-- markSubrecDirty()
-- ======================================================================
function markSubrecDirty( srcList, digestString )
  local meth = "markSubrecDirty()";
  GP=F and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

  -- Pull up the dirtyMap, find the entry for this digestString and
  -- mark it dirty.  We don't even care what the existing value used to be.
  local recMap = srcList[1];
  local dirtyMap = srcList[2];

  dirtyMap[digestString] = true;
  
  GP=F and trace("[EXIT]<%s:%s> SRC(%s)", MOD, meth, tostring(srcList) );
  return 0;
end -- markSubrecDirty()

-- ======================================================================
-- closeAllSubrecs()
-- ======================================================================
function closeAllSubrecs( srcList )
  local meth = "closeAllSubrecs()";
  GP=F and trace("[ENTER]<%s:%s> src(%s)", MOD, meth, tostring(srcList));

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

  GP=F and trace("[EXIT]: <%s:%s> : RC(%s)", MOD, meth, tostring(rc) );
  -- return rc;
  return 0; -- Mask the error for now:: TODO::@TOBY::Figure this out.
end -- closeAllSubrecs()

-- ===========================
-- End SubRecord Function Area
-- ===========================


-- ======================================================================
-- ldtInitPropMap( propMap, subDigest, topDigest, rtFlag, lsoMap )
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
function
ldtInitPropMap( propMap, esrDigest, selfDigest, topDigest, rtFlag, topPropMap )
  local meth = "ldtInitPropMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

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
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- TODO: Move this to LDT_COMMON (7/21/2013)
-- ======================================================================
function setLdtRecordType( topRec )
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
function createAndInitESR( topRec, lsoList )
  local meth = "createAndInitESR()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  local rc = 0;
  local esrRec    = aerospike:create_subrec( topRec );
  if( esrRec == nil ) then
    warn("[ERROR]<%s:%s> Problems Creating ESR", MOD, meth );
    error("[Create_ESR] Error: createAndInitESR()");
  end

  local esrDigest = record.digest( esrRec);
  local topDigest = record.digest( topRec );
  local topPropMap = lsoList[1];
  local lsoMap  = lsoList[2];

  -- Set the Property ControlMap for the ESR, and assign the parent Digest
  -- Note that we use our standard convention for property maps - all subrecs
  -- have a property map.
  -- Init the properties map for this ESR. Note that esrDigest is in here
  -- twice -- once for "self" and once for "esrRec".
  local esrPropMap = map();
  ldtInitPropMap(esrPropMap,esrDigest,esrDigest,topDigest,RT_ESR,topPropMap);

  -- NOTE: We have to make sure that the TopRec propMap also gets saved.
  esrRec[SUBREC_PROP_BIN] = esrPropMap;

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  setLdtRecordType( topRec );
  
  -- Set the record type as "ESR"
  trace("[TRACE]<%s:%s> SETTING RECORD TYPE(%s)", MOD, meth, tostring(RT_ESR));
  record.set_type( esrRec, RT_ESR );
  trace("[TRACE]<%s:%s> DONE SETTING RECORD TYPE", MOD, meth );

  GP=F and trace("[EXIT]: <%s:%s> Leaving with ESR Digest(%s)",
    MOD, meth, tostring(esrDigest));

  rc = aerospike:update_subrec( esrRec );
  if( rc == nil or rc == 0 ) then
      aerospike:close_subrec( esrRec );
  else
    warn("[ERROR]<%s:%s>Problems Updating ESR rc(%s)",MOD,meth,tostring(rc));
    error("[ESR CREATE] Error Creating System Subrecord");
  end

  return esrDigest;

end -- createAndInitESR()

-- LIST FUNCTIONS
--
--
-- ======================================================================
-- listAppend()
-- ======================================================================
-- General tool to append one list to another.   At the point that we
-- find a better/cheaper way to do this, then we change THIS method and
-- all of the LDT calls to handle lists will get better as well.
-- ======================================================================
function listAppend( baseList, additionalList )
  if( baseList == nil ) then
    warn("[INTERNAL ERROR] Null baselist in listAppend()" );
    error("[INTERNAL ERROR] Null baselist in listAppend()" );
  end
  local listSize = list.size( additionalList );
  for i = 1, listSize, 1 do
    list.append( baseList, additionalList[i] );
  end -- for each element of additionalList

  return baseList;
end -- listAppend()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
function summarizeList( myList )
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
-- ======================================================================

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- General LIST Read/Write(entry list, digest list) and LDR FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The same mechanisms are used in different LDT contexts.
-- LSET Compact List and Bin List.
-- LSTACK HotList
-- LMAP Compact List and LDR List.
-- LLIST Leaf List
-- LLIIST Node List
--
-- Each LDT basically has to search lists, insert into lists, delete
-- (Collapse) lists.  We provide general purpose methods here for
-- working with Object and Key Lists.  The difference between an ObjectList
-- and a KeyList is that the ObjectList must first undergo a "Key Extraction"
-- in order to do the key compare.

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
-- LSO Data Record (LDR) FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- LDR routines act specifically on the LDR "Data Chunk" records.

-- ======================================================================
-- ldrInsertList( ldrRec, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertList(ldrRec,lsoMap,listIndex,insertList )
  local meth = "ldrInsertList()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  GP=F and trace("[DEBUG]<%s:%s> LSO MAP(%s)", MOD, meth, tostring(lsoMap));

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
  GP=F and trace("[DEBUG]<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount);

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

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()


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
  GP=F and trace("[HEY!!]<%s:%s> Appending to Hot List(%s)", 
    MOD, meth,tostring(hotList));
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
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================
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
-- ldt_remove() -- Remove the LDT entirely from the record.
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
function ldt_remove( topRec, binName )
  local meth = "ldt_remove()";

  GP=F and trace("[ENTER]<%s:%s> binName(%s)", MOD, meth, tostring(binName));
  local rc = 0; -- start off optimistic

  -- Validate the binName before moving forward
  validateRecBinAndMap( topRec, binName, true );

  -- Extract the property map and lso control map from the lso bin list.
  local ldtList = topRec[ binName ];
  local propMap = ldtList[1];

  -- Get the ESR and delete it.
  local esrDigest = propMap[PM_EsrDigest];
  local esrDigestString = tostring(esrDigest);
  local esrRec = aerospike:open_subrec( topRec, esrDigestString );
  rc = aerospike:delete( esrRec );
  if( rc < 0 ) then
    warn("[ESR DELETE ERROR]: Can't Delete: Bin(%s)", MOD, meth, binName);
    error("[ESR DELETE ERROR] Cannot Delete Subrec");
  end

  topRec[binName] = nil;

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM_Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
    MOD, meth );
    error("[INTERNAL ERROR]: Invalid Record LDT Control Bin");
  end
  local ldtCount = recPropMap[RPM_LdtCount];
  if( ldtCount <= 1 ) then
    -- Remove this bin
    topRec[REC_LDT_CTRL_BIN] = nil;
  else
    recPropMap[RPM_LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
  end

  rc = aerospike:update( topRec );

  return rc;
end -- ldt_remove()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
