-- Large Set (LSET) Design
-- (May 20, 2013) (Last Updated)
--
-- ======================================================================
-- Please refer to lset_design.lua for architecture and design notes.
-- Aerospike Large Set (LSET) Operations
-- (*) lset_create():
--          :: Create the LSET object in the bin specified, using the
--          :: creation arguments passed in (probably a package).
-- (*) lset_create_and_insert():
--          :: Insert an item into the Large Set, Create first if needed.
--          :: Apply the creation package on create.
-- (*) lset_insert(): Insert an item into the Large Set
-- (*) lset_search(): Search for an item in the Large Set
-- (*) lset_search_then_filter(): Search for an item in the set
--          :: with an additional search filter (for complex objects).
-- (*) lset_exists(): Test Existence on an item in the set
-- (*) lset_exists_then_filter(): Test Existence on an item in the set
--          :: with an additional search filter (for complex objects)
-- (*) lset_delete(): Delete an item from the set
-- (*) lset_delete_then_filter(): QUESTION:: DO WE NEED THIS????
-- (*) lset_config(): retrieve all current config settings in map format
-- (*) lset_size():   Return the size (e.g. item count) of the Set
--
-- ======================================================================
-- LSET Design and Type Comments:
--
-- The LSET value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information and a set of record bins that contain
-- lists -- in order to split up the search across many lists.
--
-- Enhancements to LSET :
-- 
-- 1. New: 
--    Each top-record containing an lset-bin will have a property-map that is 
--    common to the entire record. 
-- 2. New: 
--    Each lset bin by itself will also have a LDT bin-specific property map.   
-- 3. New: 
--    There will be a separate existence sub-record (ESR) that indicates 
--    whether a list of LDT child-records are valid or not. 
-- 4. Modification: 
--    The LSET control-bin which was a simple map earlier has now been changed
--    into a list. 
--    a. The first entry of the list contains the ldt-bin specific 
--       property map referred in item 2. 
--    b. The second entry of the list contains the original lsetCtrlmap which has
--       been a map of the standard lset attributes and their values 
-- 5. Modification: 
--    Abbreviated names for the lset record attributes to save storage space: 
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
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large SET control information
-- is named "LSetControlBin", and the list bins are prefixed with
-- "LSetBin_" and numbered from 0 to N, where N is the modulo value
-- that is set on create (otherwise, default to 31).
-- (*) LSET Control Info:
-- (*) LSET Bins.
--
-- (Standard Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |. . .|LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2|     |CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |     |           |                   
--                      V     V     V           V                   
--                   +=====++===+ +===+       +===+                  
--                   | Map ||val| |val|       |val|
--                   +=====+|val| |val|       |val|
--                          |...| |...|       |...|
--                          |val| |val|       |val|
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
-- |User |User |. . .|LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2|     |CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |   
--                      V     V  
--                   +=====++===+
--                   | Map ||val|
--                   +=====+|val|
--                          |...|
--                          |val|
--                          +===+
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || LSET Bin CONTENTS  |||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- In the LSET bin (LSetControlBin), there is a Map object:
-- accessed, as follows:
-- lsetCtrlMap.Magic = "MAGIC"; -- this verifies we have a valid map
-- lsetCtrlMap.ItemCount = 0;   -- Count of valid entries
-- lsetCtrlMap.TotalCount = 0;  -- Count of all entry positions, incl deletes

-- ======================================================================
-- The Contents of the Control Map for LSET is captured in the LSET
-- "initialize()" function -- so we use that here to show contents of the
-- LSET control map.

-- ======================================================================
-- initializeLSetMap:
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
-- (*) lsetBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized lsetCtrlMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLSetMap(topRec, lsetBinName )
  local meth = "initializeLSetMap()";
  GP=F and trace("[ENTER]:<%s:%s>::Bin(%s)",MOD, meth, tostring(lsetBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsetCtrlMap = map();
  -- General Parameters
  lsetCtrlMap.LdtType = LDT_LSET; -- Mark this as a Large Set
  lsetCtrlMap.Magic = MAGIC;
  lsetCtrlMap.ExistSubRecDig = 0; -- Pt to the LDT "Exists" subrecord (digest)
  lsetCtrlMap.Transform = nil;
  lsetCtrlMap.UnTransform = nil;
  lsetCtrlMap.KeyCompare = nil; -- Key Func used only in complex mode
  lsetCtrlMap.StoreState = SS_COMPACT; -- always start in "compact mode"
  lsetCtrlMap.StoreMode = SM_LIST; -- Use List Mode
  lsetCtrlMap.BinaryStoreSize = 0; -- only used in "binary" StoreMode.
  lsetCtrlMap.KeyType = KT_ATOMIC; -- assume "atomic" values for now.
  -- NOTE: We are still hard-coding the Bin Name:
  -- lsetCtrlMap.BinName = lsetBinName;
  lsetCtrlMap.BinName = LSET_CONTROL_BIN;
  lsetCtrlMap.ItemCount = 0;   -- Count of valid elements
  lsetCtrlMap.TotalCount = 0;  -- Count of both valid and deleted elements
  lsetCtrlMap.Modulo = DEFAULT_DISTRIB;
  lsetCtrlMap.ThreshHold = 101; -- Rehash after this many have been inserted

  GP=F and trace("[ENTER]: <%s:%s>:: lsetCtrlMap(%s)",
                 MOD, meth, tostring(lsetCtrlMap));

  return lsetCtrlMap;
end -- initializeLSetMap()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
