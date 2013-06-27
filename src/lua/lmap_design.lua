-- Large Map (LMAP) Design
-- (June 26, 2013) Version 3 (Last Updated)
-- 
--=============================================================================
-- LMAP Design and Type Comments:
--=============================================================================
--
-- What is LMAP ?
-- 
-- The LMAP value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information and a set of record bins that contain
-- fixed-sized list of digests which directly point to a record. LMAP can be
-- viewed as a version of LSET object which points to a fixed-size warm-list
-- of digests, instead of having a Lset control-bin pointing to a map object 
-- and additional Lset bins (1 to N) each pointing to list-values. 
-- The LMAP object does not have a separate control-bin and value-bins.
-- We just have one direct LMAP bin object. 
--
-- Advantages over LSET :
--
-- 1. Lmap gets rid of a separate control-bin, instead control-information is 
--    at the top of each lmap map. 
-- 2. Lmap achieves vertical-scaling by having a list of digests, each of which
--    points to a LDR, instead of a list of values. 
-- 3. Users can add as many lmap-type bins to a record and customize the names
--    of the records. In the case of LSET, there can be only one lset-type bin
--    in a record and their names are fixed.
--
-- Enhancements to LMAP :
-- 
-- 1. New: 
--    Each top-record containing an lmap-bin will have a property-map that is 
--    common to the entire record. 
-- 2. New: 
--    Each lmap bin will also have a LDT bin-specific property map.   
-- 3. New: 
--    There will be a separate existence sub-record (ESR) that indicates 
--    whether a list of LDT child-records are valid or not. 
-- 4. Modification: 
--    The LMAP control-bin which was a simple map earlier has now been changed
--    into a list. 
--    The first entry of the list contains the ldt-bin specific 
--    property map referred in item 2. 
--    The second entry of the list contains the original lsomap which has been
--    a map of many lmap attributes and their values 
-- 5. Modification: 
--    Abbreviated names for the lmap record attributes to save storage space
--
--    ******************  Description copied-over from lstack.lua *********
--
----   + Since Lua wraps up the LDT Control map as a self-contained object,
--     we are paying for storage in EACH LDT Bin for the map field names. 
--     Thus, even though we like long map field names for readability:
--     e.g.  lsoMap.HotEntryListItemCount, we don't want to spend the
--     space to store the large names in each and every LDT control map.
--     So -- we do another Lua Trick.  Rather than name the key of the
--     map value with a large name, we instead use a single character to
--     be the key value, but define a descriptive variable name to that
--     single character.  So, instead of using this in the code:
--     lsoMap.ListItemCount = 50;
--            123456789012345678901
--     (which would require 21 bytes of storage); We instead do this:
--     local ListItemCount='C';
--     lsoMap[ListItemCount] = 50;
--     Now, we're paying the storage cost for 'C' (1 byte) and the value.
--
--     So -- we have converted all of our LDT lua code to follow this
--     convention (fields become variables the reference a single char)
--     and the mapping of long name to single char will be done in the code.
--      
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- 
--
--(Standard Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |. .  |LMAP |LMAP |. . .|LMAP |                |
-- |Bin 1|Bin 2|     |Bin  |Bin  |     |Bin  |                |
-- |     |     |. .  |Name |Name |. . .|Name |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |-------------------------------------------->+---------+                      
--                      V                     								| LMAP    |    
--                    +---------+             								| control |
--   		          | LMAP    |            							    | bin     |
--                    | control |			  								| bin     |
--                    | bin     |		      								+---------+ 						 LDR 1	
--   		          +---------+                          LDR 1			|Digest 1 |	+---------------------->+--------+							
--         	          |Digest 1 |+----------------------->+--------+	    |---------|              LDR 2      |Entry 1 |
--                    |---------|              LDR 2      |Entry 1 |		|Digest 2 |+------------>+--------+ |Entry 2 |
--                    |Digest 2 |+------------>+--------+ |Entry 2 |	    +---------+              |Entry 1 | |   o    |
--                    +---------+              |Entry 1 | |   o    |	    | o o o   |              |Entry 2 | |   o    |
--                    | o o o   |              |Entry 2 | |   o    |		|---------|				 |   o    |	|Entry n |
--                    |---------|    LDR N     |   o    | |   o    |		|Digest N |				 |Entry n |	+--------+
--                    |Digest N |+->+--------+ |   o    | |Entry n |		+---------+				 +--------+
--                    +---------+   |Entry 1 | |   o    | +--------+
--                                  |Entry 2 | |Entry n |
--                                  |   o    | +--------+
--                                  |   o    |
--                                  |   o    |
--                                  |Entry n |
--                                  +--------+
--
--  
--
-- (Compact Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |. .  |LMAP |                
-- |Bin 1|Bin 2|     |Bin  |                
-- |     |     |. .  |Name |                  
-- +-----+-----+-----+-----+----------------------------------------+
--                      |        
--                      V       
--                    +---------+                                 
--                    | LMAP    |
--                    | control |
--                    | info    |
--   		          +---------+                          LDR 1
--         	          |Digest 1 |+----------------------->+--------+
--                    |---------|              LDR 2      |Entry 1 |
--                    |Digest 2 |+------------>+--------+ |Entry 2 |
--                    +---------+              |Entry 1 | |   o    |
--                    | o o o   |              |Entry 2 | |   o    |
--                    |---------|    LDR N     |   o    | |   o    |
--                    |Digest N |+->+--------+ |   o    | |Entry n |
--                    +---------+   |Entry 1 | |   o    | +--------+
--                                  |Entry 2 | |Entry n |
--                                  |   o    | +--------+
--                                  |   o    |
--                                  |   o    |
--                                  |Entry n |
--                                  +--------+
--
--=============================================================================
-- How does LMAP work ?
--
-- The Large Map data structure has a control-bin (with a user-defined name) 
-- which points to a list of digests. 
-- 
-- a. In the compact mode, there is only one LMAP bin pointing to a list of N
--    digest items. We compute the digest for the record, hash the digest over
--    the list of N bins, hash(digest) Modulo N, insert the digest in the 
--    appropriate list-index of the first and only lmap bin with a pointer 
--    to the actual LDR. 
--
-- b. In the standard mode, there are multiple LMAP bins in a record. When 
--    an LDR needss to be inserted, we first pick the matching lmap bin-name
--    and then proceed to hashing the digest and finding its place in the list. 
--
-- c. Unlike lstack which grows as a list but is read in reverse as a stack, 
--    lmap digest entries are meant to behave as a simple linear list. So a 
-- 	  lmap search is very much similar to the lset-search of hash-matching, 
-- 	  except that in the case of lset, we would hash to find the correct bin, 
--    but in the case of lmap, we look-up by bin-name, but hash to find the 
--    digest entry amongst the list-indices. 
--  
---- ======================================================================
-- 
-- Aerospike Large Map (LMAP) Operations :
--
-- (*) lmap_create():
--          :: Create the LMAP object in the bin specified, using the
--          :: creation arguments passed in (probably a package).
-- (*) lmap_create_and_insert():
--          :: Insert an item into the Large Map, Create first if needed.
--          :: Apply the creation package on create.
-- (*) lmap_insert(): Insert an item into the Large Map
-- (*) lmap_search(): Search for an item in the Large Map
-- (*) lmap_search_then_filter(): Search for an item in the map
--          :: with an additional search filter (for complex objects).
-- (*) lmap_exists(): Test Existence on an item in the map
-- (*) lmap_exists_then_filter(): Test Existence on an item in the map
--          :: with an additional search filter (for complex objects)
-- (*) lmap_delete(): Delete an item from the map
-- (*) lmap_delete_then_filter(): QUESTION:: DO WE NEED THIS????
-- (*) lmap_config(): retrieve all current config settings in map format
-- (*) lmap_size():   Return the size (e.g. item count) of the map
--
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || LMAP Bin CONTENTS  |||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- 
-- The definition of control-info of the main record (top rec) and the subrecords
-- is essentially what we use to initialize the fixed-size lmap digest list, 
-- so we use the init routines to show what's in the lmap-control info field. 

-- ======================================================================
-- initializeLMap:
-- ======================================================================
-- Set up the LMap bin with the standard (default) values.
-- These values may later be overridden by the user.
-- One of the elements of this structure is a pointer to the LMAP control info. 
-- This single structure contains ALL of the settings/parameters that drive the
-- LMAP behavior.  Thus this function represents the data-type LMAP - all the 
-- control-fields are defined here too. 
-- ======================================================================
local function initializeLMap( topRec, lmapBinName )
  local meth = "initializeLMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    MOD, meth, tostring(lmapBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lmapCtrl = map();
  -- General LSO Parms:
  lmapCtrl.ItemCount = 0;         -- A count of all items in the stack
  lmapCtrl.Version = 1 ;          -- Current version of the code
  lmapCtrl.LdtType = "LMAP";      -- identify ldt variant
  lmapCtrl.Magic = MAGIC;         -- we will use this to verify we have a valid map
  lmapCtrl.BinName = lmapBinName; -- Defines the LSO Bin
  lmapCtrl.NameSpace = "test";    -- Default NS Name -- to be overridden by user
  lmapCtrl.Set = "set";           -- Default Set Name -- to be overridden by user
  lmapCtrl.StoreMode = SM_LIST;   -- SM_LIST or SM_BINARY:
  lmapCtrl.ExistSubRecDig = 0;    -- Pt to the LDT "Exists" subrecord (digest)

  -- LDR Chunk Settings (of type LMAP): Passed into "Chunk Create"
  lmapCtrl.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lmapCtrl.LdrByteEntrySize =  0;   -- Byte size of a fixed size Byte Entry
  lmapCtrl.LdrByteCountMax =   0;   -- Max # of BYTES in a Data Chunk (binary mode)

  -- Digest List Settings: List of Digests of Large Data Records
  lmapCtrl.DigestList = list(); -- the list of digests for LDRs
  lmapCtrl.TopFull = false; -- true when top chunk is full (for next write)
  lmapCtrl.ListDigestCount = 0; -- Number of  Data Record Chunks
  lmapCtrl.ListMax = 100; -- Number of  Data Record Chunks
  lmapCtrl.TopChunkEntryCount = 0; -- Count of entries in top-most LDR chunk
  lmapCtrl.TopChunkByteCount = 0; -- Count of bytes used in top-most LDR Chunk

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth , tostring(lmapCtrl));

  -- Put our new map in the record, then store the record.
  topRec[lmapBinName] = lmapCtrl;

  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return lmapCtrl
end -- initializeLsoMap

-- ======================================================================
-- initializeLdrMap( ldrMap )
-- ======================================================================
-- Set the values in a LMAP Data Record (LDR) Control Bin map. LDR Records
-- hold the actual data for the digest-list.
-- This function represents the "type" LDR MAP -- all fields are
-- defined here.
-- There are potentially three bins in an LDR Record:
-- (1) ldrRec[LDR_CTRL_BIN]: The control Map (defined here)
-- (2) ldrRec[LDR_LIST_BIN]: The Data Entry List (when in list mode)
-- (3) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
-- ======================================================================
local function initializeLdrMap( topRec, ldrRec, ldrMap, lmapCtrl )
  local meth = "initializeLdrMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  ldrMap.ParentDigest = record.digest( topRec );
  ldrMap.StoreMode = lmapCtrl.StoreMode;
  ldrMap.Digest = record.digest( ldrRec );
  ldrMap.ListEntryMax = lmapCtrl.LdrEntryCountMax; -- Max entries in value list
  ldrMap.ByteEntrySize = lmapCtrl.LdrByteEntrySize; -- ByteSize of Fixed Entries
  ldrMap.ByteEntryCount = 0;  -- A count of Byte Entries
  ldrMap.ByteCountMax = lmapCtrl.LdrByteCountMax; -- Max # of bytes in ByteArray
  ldrMap.Version = lmapCtrl.Version;
  ldrMap.LogInfo = 0;
end -- initializeLdrMap()


-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
