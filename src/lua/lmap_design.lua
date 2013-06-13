-- Large Map (LMAP) Design
-- (May 28, 2013) (Last Updated)
--
-- ======================================================================
-- Aerospike Large Map (LMAP) Operations
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
-- LMAP Design and Type Comments:
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
-- Advantages :
-- 1. Lmap gets rid of a separate control-bin, instead control-information is 
--    at the top of each lmap map. 
-- 2. Lmap achieves vertical-scaling by having a list of digests, instead of 
--    a list of values. 
-- 3. There can be only one lset-type bin in a record, their names are fixed.
--    Users can add as many lmap-type bins to a record and customize the names
--    of the records.  
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large Map list-bin takes on a name
-- chosen by the user. 
-- 
-- TODO : Design for multiple lmap bins in a record
--
--(Standard Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |. .  |LMAP |LMAP |. . .|LMAP |                |
-- |Bin 1|Bin 2|     |Bin  |Bin  |     |Bin  |                |
-- |     |     |. .  |Name |Name |. . .|Name |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |                             
--                      V                                  
--                    +---------+                                 
--                    | LMAP    |
--                    | control |
--                    | bin     |
--   		      +---------+                          LDR 1
--         	      |Digest 1 |+----------------------->+--------+
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
-- ******************** TBD *************************************
--
-- The Large Map distributes searches over N lists.  Searches are done
-- with linear scan in one of the bin lists.  The set values are hashed
-- and then the specific bin is picked "hash(val) Modulo N".  The N bins
-- are organized by name using the method:  prefix "LsetBin_" and the
-- modulo N number.
--
-- The modulo number is always a prime number -- to minimize the amount
-- of "collisions" that are often found in power of two modulo numbers.
-- Common choices are 17, 31 and 61.
--
-- The initial state of the LMAP is "Compact Mode", which means that ONLY
-- ONE LIST IS USED -- in Bin 0.  Once there are a "Threshold Number" of
-- entries, the Bin 0 entries are rehashed into the full set of bin lists.
-- Note that a more general implementation could keep growing, using one
-- of the standard "Linear Hashing-style" growth patterns.
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
--   		      +---------+                          LDR 1
--         	      |Digest 1 |+----------------------->+--------+
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
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || LMAP Bin CONTENTS  |||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- 
-- The definition of control-info of the main record (top rec) and
-- the subrecords is essentially what we use to initialize the fixed-size 
-- warm-list, so we use the init routines to show what's in those control maps

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
  local lmap = map();
  -- General LSO Parms:
  lmap.ItemCount = 0;         -- A count of all items in the stack
  lmap.Version = 1 ;          -- Current version of the code
  lmap.LdtType = "LMAP";      -- identify ldt variant
  lmap.Magic = MAGIC;         -- we will use this to verify we have a valid map
  lmap.BinName = lmapBinName; -- Defines the LSO Bin
  lmap.NameSpace = "test";    -- Default NS Name -- to be overridden by user
  lmap.Set = "set";           -- Default Set Name -- to be overridden by user
  lmap.StoreMode = SM_LIST;   -- SM_LIST or SM_BINARY:
  lmap.ExistSubRecDig = 0;    -- Pt to the LDT "Exists" subrecord (digest)

  -- LDR Chunk Settings (of type LMAP): Passed into "Chunk Create"
  lmap.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lmap.LdrByteEntrySize =  0;   -- Byte size of a fixed size Byte Entry
  lmap.LdrByteCountMax =   0;   -- Max # of BYTES in a Data Chunk (binary mode)

  -- Digest List Settings: List of Digests of Large Data Records
  lmap.DigestList = list(); -- the list of digests for LDRs
  lmap.TopFull = false; -- true when top chunk is full (for next write)
  lmap.ListDigestCount = 0; -- Number of  Data Record Chunks
  lmap.ListMax = 100; -- Number of  Data Record Chunks
  lmap.TopChunkEntryCount = 0; -- Count of entries in top-most LDR chunk
  lmap.TopChunkByteCount = 0; -- Count of bytes used in top-most LDR Chunk

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth , tostring(lmap));

  -- Put our new map in the record, then store the record.
  topRec[lmapBinName] = lmap;

  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return lmap
end -- initializeLsoMap

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
