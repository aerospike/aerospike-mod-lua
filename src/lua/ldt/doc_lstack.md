# LSTACK Main Functions

The following external functions are defined in the LSTACK module:

* Status = push( topRec, ldtBinName, newValue, userModule )
* Status = push_all( topRec, ldtBinName, valueList, userModule )
* List   = peek( topRec, ldtBinName, peekCount ) 
* List   = pop( topRec, ldtBinName, popCount ) 
* List   = scan( topRec, ldtBinName )
* List   = filter( topRec, ldtBinName, peekCount,userModule,filter,fargs)
* Status = destroy( topRec, ldtBinName )
* Number = size( topRec, ldtBinName )
* Map    = get_config( topRec, ldtBinName )
* Status = set_capacity( topRec, ldtBinName, new_capacity)
* Status = get_capacity( topRec, ldtBinName )

LSTACK Design and Type Comments (aka Large Stack Object, or LSO).
The lstack type is a member of the new Aerospike Large Type family,
Large Data Types (LDTs).  LDTs exist only on the server, and thus must
undergo some form of translation when passing between client and server.

LSTACK is a server side type that can be manipulated ONLY by this file,
lstack.lua.  We prevent any other direct manipulation -- any other program
or process must use the lstack api provided by this program in order to

An LSTACK value -- stored in a record bin -- is represented by a Lua MAP
object that comprises control information, a directory of records
(for "warm data") and a "Cold List Head" ptr to a linked list of directory
structures that each point to the records that hold the actual data values.

# Visual Depiction

In a user record, the bin holding the Large Stack Object (LSTACK) is
referred to as an "LSTACK" bin. The overhead of the LSTACK value is:

* LSTACK Control Info (~70 bytes)
* LSTACK Hot Cache: List of data entries (on the order of 100)
* LSTACK Warm Directory: List of Aerospike Record digests:
    100 digests(250 bytes)
* LSTACK Cold Directory Head (digest of Head plus count) (30 bytes)
* Total LSTACK Record overhead is on the order of 350 bytes

NOTES:

* In the Hot Cache, the data items are stored directly in the
    cache list (regardless of whether they are bytes or other as_val types)
* In the Warm Dir List, the list contains aerospike digests of the
    LSTACK Data Records (LDRs) that hold the Warm Data.  The LDRs are
    opened (using the digest), then read/written, then closed/updated.
* The Cold Dir Head holds the Aerospike Record digest of a record that
    holds a linked list of cold directories.  Each cold directory holds
    a list of digests that are the cold LSTACK Data Records.
* The Warm and Cold LSTACK Data Records use the same format -- so they
    simply transfer from the warm list to the cold list by moving the
    corresponding digest from the warm list to the cold list.
* Record types used in this design:
1. There is the main record that contains the LSTACK bin (LSTACK Head)
2. There are LSTACK Data "Chunk" Records (both Warm and Cold)
    ==> Warm and Cold LSTACK Data Records have the same format:
        They both hold User Stack Data.
3. There are Chunk Directory Records (used in the cold list)

* How it all connects together....
- The main record points to:
    -- Warm Data Chunk Records (these records hold stack data)
    -- Cold Data Directory Records (these records hold ptrs to Cold Chunks)

* We may have to add some auxilliary information that will help
    pick up the pieces in the event of a network/replica problem, where
    some things have fallen on the floor.  There might be some "shadow
    values" in there that show old/new values -- like when we install
    a new cold dir head, and other things.  TBD
    
    
```

+-----+-----+-----+-----+----------------------------------------+
|User |User |o o o|LSO  |                                        |
|Bin 1|Bin 2|o o o|Bin 1|                                        |
+-----+-----+-----+-----+----------------------------------------+
                 /       \                                       
  ================================================================
    LSTACK Map                                              
    +-------------------+                                 
    | LSO Control Info  | About 20 different values kept in Ctrl Info
    |...................|
    |...................|< Oldest ................... Newest>            
    +-------------------+========+========+=======+=========+
    |<Hot Entry Cache>  | Entry 1| Entry 2| o o o | Entry n |
    +-------------------+========+========+=======+=========+
    |...................|HotCache entries are stored directly in the record
    |...................| 
    |...................|WarmCache Digests are stored directly in the record
    |...................|< Oldest ................... Newest>            
    +-------------------+========+========+=======+=========+
    |<Warm Digest List> |Digest 1|Digest 2| o o o | Digest n|
    +-------------------+===v====+===v====+=======+====v====+
 +-<@>Cold Dir List Head|   |        |                 |    
 |  +-------------------+   |        |                 |    
 |                    +-----+    +---+      +----------+   
 |                    |          |          |     Warm Data(WD)
 |                    |          |          |      WD Rec N
 |                    |          |          +---=>+--------+
 |                    |          |     WD Rec 2   |Entry 1 |
 |                    |          +---=>+--------+ |Entry 2 |
 |                    |      WD Rec 1  |Entry 1 | |   o    |
 |                    +---=>+--------+ |Entry 2 | |   o    |
 |                          |Entry 1 | |   o    | |   o    |
 |                          |Entry 2 | |   o    | |Entry n |
 |                          |   o    | |   o    | +--------+
 |                          |   o    | |Entry n |
 |                          |   o    | +--------+
 |                          |Entry n | "LDR" (LSTACK Data Record) Pages
 |                          +--------+ [Warm Data (LDR) Chunks]
 |                                            
 |                                            
 |                           <Newest Dir............Oldest Dir>
 +-------------------------->+-----+->+-----+->+-----+-->+-----+-+
  (DirRec Pages DoubleLink)<-+Rec  |<-+Rec  |<-+Rec  | <-+Rec  | V
   The cold dir is a linked  |Chunk|  |Chunk|  |Chunk| o |Chunk|
   list of dir pages that    |Dir  |  |Dir  |  |Rec  | o |Dir  |
   point to LSO Data Records +-----+  +-----+  +-----+   +-----+
   that hold the actual cold [][]:[]  [][]:[]  [][]:[]   [][]:[]
   data (cold chunks).       +-----+  +-----+  +-----+   +-----+
                              | |  |   | |  |   | |  |    | |  |
   LDRS (per dir) have age:   | |  V   | |  V   | |  V    | |  V
   <Oldest LDR .. Newest LDR> | |::+--+| |::+--+| |::+--+ | |::+--+
   As "Warm Data" ages out    | |::|Cn|| |::|Cn|| |::|Cn| | |::|Cn|
   of the Warm Dir List, the  | V::+--+| V::+--+| V::+--+ | V::+--+
   LDRs transfer out of the   | +--+   | +--+   | +--+    | +--+
   Warm Directory and into    | |C2|   | |C2|   | |C2|    | |C2|
   the cold directory.        V +--+   V +--+   V +--+    V +--+
                              +--+     +--+     +--+      +--+
   The Warm and Cold LDRs     |C1|     |C1|     |C1|      |C1|
   have identical structure.  +--+     +--+     +--+      +--+
                               A        A        A         A    
                               |        |        |         |
    [Cold Data (LDR) Chunks]---+--------+--------+---------+

```

The "Hot Entry Cache" is the true "Top of Stack", holding roughly the
top 50 to 100 values.  The next level of storage is found in the first
Warm dir list (the last Chunk in the list).  Since we process stack
operations in LIFO order, but manage them physically as a list
(append to the end), we basically read the pieces in top down order,
but we read the CONTENTS of those pieces backwards.  It is too expensive
to "prepend" to a list -- and we are smart enough to figure out how to
read an individual page list bottom up (in reverse append order).

We don't "age" the individual entries out one at a time as the Hot Cache
overflows -- we instead take a group at a time (specified by the
HotCacheTransferAmount), which opens up a block of empty spots. Notice that
the transfer amount is a tuneable parameter -- for heavy reads, we would
want MORE data in the cache, and for heavy writes we would want less.

If we generally pick half (e.g. 100 entries total, and then transfer 50 at
a time when the cache fills up), then half the time the inserts will affect
ONLY the Top (LSTACK) record -- so we'll have only one Read, One Write 
operation for a stack push.  1 out of 50 will have the double read,
double write, and 1 out of 10,000 (or so) will have additional
IO's depending on the state of the Warm/Cold lists.
Notice ALSO that when we use a coupled Namespace for LDTs (main memory
for the top records and SSD for the subrecords), then 49 out of 50
writes and small reads will have ZERO I/O cost -- since it will be
contained in the main memory record.

NOTES:
Design, V3.x.  For really cold data -- things out beyond 50,000
elements, it might make sense to just push those out to a real disk
based file (to which we could just append -- and read in reverse order).
If we ever need to read the whole stack, we can afford
the time and effort to read the file (it is an unlikely event).  The
issue here is that we probably have to teach Aerospike how to transfer
(and replicate) files as well as records.

Design, V3.x. We will need to limit the amount of data that is held
in a stack. We've added "StoreLimit" to the ldtMap, as a way to limit
the number of items.  Note that this can be used to limit both the
storage and the read amounts.
One way this could be used is to REUSE a cold LDR page when an LDR
page is about to fall off the end of the cold list.  However, that
must be considered carefully -- as the time and I/O spent messing
with the cold directory and the cold LDR could be a performance hit.
We'll have to consider how we might age these pages out gracefully
if we can't cleverly reuse them (patent opportunity here).


NOTE THAT ALL INSERTS ARE INTO HOT LIST -- and transforms are done
there.  All UNTRANSFORMS are done reading from the List (Hot List or
warm/cold Data Page List).  Notice that even though the values may be
transformed (compacted into) bytes, they are still just inserted into
the hot list, we don't try to pack them into an array;
that is done only in the warm/cold pages (where the benefit is greater).

Read Filters are applied AFTER the UnTransform (bytes and list).

NOTE: New changes with V4.3 to Push and Peek:

* Stack Push has an IMPLICIT transform function -- which is defined
    in the create spec.  So, the two flavors of Stack Push are now
    + lstack_push(): with implicit transform when defined
    + lstack_create_and_push(): with the ability to create as
      needed -- and with the supplied create_spec parameter.
* Stack Peek has an IMPLICIT UnTransform function -- which is defined
    in the create spec.  So, the two flavors of Stack Peek are now
    + lstack_peek(): with implicit untransform, when defined in create.
    + lstack_peek_then_filter(): with implicit untransform and a filter
      to act as an additional query mechanism.

On Create, a Large Stack Object can be configured with a Transform function,
to be used on storage (push) and an UnTransform function, to be used on
retrieval (peek):

* stack_push(): Push a user value (AS_VAL) onto the stack, 
    calling the Transform on the value FIRST to transform it before
    storing it on the stack.
* stack_peek_then_filter: Retrieve N values from the stack, and for each
    value, apply the transformation/filter UDF to the value before
    adding it to the result list.  If the value doesn't pass the
    filter, the filter returns nil, and thus it would not be addedto the 
    result list.
    

# Aerospike Server Functions:

Aerospike Record Functions:

* status = aerospike:create( topRec )
* status = aerospike:update( topRec )
* status = aerospike:remove( rec ) (not currently used)

Aerospike SubRecord Functions:

* newRec = aerospike:create_subrec( topRec )
* rec    = aerospike:open_subrec( topRec, childRecDigest)
* status = aerospike:update_subrec( childRec )
* status = aerospike:close_subrec( childRec )
* status = aerospike:remove_subrec( subRec )  

Record Functions:

* digest = record.digest( childRec )
* status = record.set_type( topRec, recType )
* status = record.set_flags( topRec, binName, binFlags )


# FUNCTION TABLE

Table of Functions: Used for Transformation and Filter Functions.
This is held in UdfFunctionTable.lua.  Look there for details.

## External Modules

Get addressability to the Function Table: Used for compress and filter
local functionTable = require('ldt/UdfFunctionTable');

Common LDT functions that are used by ALL of the LDTs:

* local LDTC = require('ldt/ldt_common');
* local ldte=require('ldt/ldt_errors');

We have a set of packaged settings for each LDT:

* local lstackPackage = require('ldt/settings_lstack');


## GLOBAL CONSTANTS

```
local MAGIC="MAGIC";     -- the magic value for Testing LSTACK integrity

Default storage limit for a stack -- can be overridden by setting
one of the packages.
local G_STORE_LIMIT = 100000  -- Store no more than this.  User can override.

StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

Record Types -- Must be numbers, even though we are eventually passing
in just a "char" (and int8_t).
NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
come back to bite me.
(1) As a flag in record.set_type() -- where the index bits need to show
    the TYPE of record (CDIR NOT used in this context)
(2) As a TYPE in our own propMap[PM_RecType] field: CDIR *IS* used here.
local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
local RT_CDIR= 3; -- xxx: Cold Dir Subrec::Not used for set_type() 
local RT_ESR = 4; -- 0x4: Existence Sub Record

Bin Flag Types -- to show the various types of bins.
NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

LDT TYPES (only lstack is defined here)
local LDT_TYPE_LSTACK = "LSTACK";

Errors used in LDT Land
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

We maintain a pool, or "context", of subrecords that are open.  That allows
us to look up subrecs and get the open reference, rather than bothering
the lower level infrastructure.  There's also a limit to the number
of open subrecs.
local G_OPEN_SR_LIMIT = 20;

When the user wants to override the default settings, or register some
functions, the user module with the "adjust_settings" function will be
used.
local G_SETTINGS = "adjust_settings";

```

## INTERNAL BIN NAMES
The Top Rec LDT bin is named by the user -- so there's no hardcoded name
for each used LDT bin.

In the main record, there is one special hardcoded bin -- that holds
some shared information for all LDTs.
Note the 14 character limit on Aerospike Bin Names.
>> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local REC_LDT_CTRL_BIN  = "LDTCONTROLBIN"; -- Single bin for all LDT in rec

There are THREE different types of (Child) subrecords that are associated
with an LSTACK LDT:
(1) LDR (LSTACK Data Record) -- used in both the Warm and Cold Lists
(2) ColdDir Record -- used to hold lists of LDRs (the Cold List Dirs)
(3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
Each Subrecord has some specific hardcoded names that are used

All LDT subrecords have a properties bin that holds a map that defines
the specifics of the record and the LDT.
NOTE: Even the TopRec has a property map -- but it's stashed in the
user-named LDT Bin
>> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN   = "SR_PROP_BIN";

The LDT Data Records (LDRs) use the following bins:
The SUBREC_PROP_BIN mentioned above, plus
>> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_LIST_BIN      = "LdrListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

The Cold Dir Records use the following bins:
The SUBREC_PROP_BIN mentioned above, plus
>> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local COLD_DIR_LIST_BIN = "ColdDirListBin"; 
local COLD_DIR_CTRL_BIN = "ColdDirCtrlBin";

The Existence Sub-Records (ESRs) use the following bins:
The SUBREC_PROP_BIN mentioned above (and that might be all)

# Internal Structure

There are four main Record Types used in the LSTACK Package, and their
initialization functions follow.  The initialization functions
define the "type" of the control structure:

* TopRec: the top level user record that contains the LSTACK bin
* EsrRec: The Existence SubRecord (ESR) that coordinates all child
            subrecs for a given LDT.
* LdrRec: the LSTACK Data Record (LDR) that holds user Data.
* ColdDirRec: The Record that holds a list of Sub Record Digests
    (i.e. record pointers) to the LDR Data Records.  The Cold list is
    a linked list of Directory pages;  each dir contains a list of
    digests (record pointers) to the LDR data pages.
<+> Naming Conventions:
  + All Field names (e.g. ldtMap[StoreMode]) begin with Upper Case
  + All variable names (e.g. ldtMap[StoreMode]) begin with lower Case
  + As discussed below, all Map KeyField names are INDIRECTLY referenced
    via descriptive variables that map to a single character (to save
    space when the entire map is msg-packed into a record bin).
  + All Record Field access is done using brackets, with either a
    variable or a constant (in single quotes).
    (e.g. topRec[binName] or ldrRec[LDR_CTRL_BIN]);

<+> Recent Change in LdtMap Use: (6/21/2013 tjl)
  + In order to maintain a common access mechanism to all LDTs, AND to
    limit the amount of data that must be "un-msg-packed" when accessed,
    we will use a common property map and a type-specific property map.
    That means that the "ldtMap" that was the primary value in the LsoBin
    is now a list, where ldtCtrl[1] will always be the propMap and
    ldtCtrl[2] will always be the ldtMap.  In the server code, using "C",
    we will sometimes read the ldtCtrl[1] (the property map) in order to
    perform some LDT management operations.
  + Since Lua wraps up the LDT Control map as a self-contained object,
    we are paying for storage in EACH LDT Bin for the map field names. 
    Thus, even though we like long map field names for readability:
    e.g.  ldtMap.HotEntryListItemCount, we don't want to spend the
    space to store the large names in each and every LDT control map.
    So -- we do another Lua Trick.  Rather than name the key of the
    map value with a large name, we instead use a single character to
    be the key value, but define a descriptive variable name to that
    single character.  So, instead of using this in the code:
    ldtMap.HotEntryListItemCount = 50;
           123456789012345678901
    (which would require 21 bytes of storage); We instead do this:
    local HotEntryListItemCount='H';
    ldtMap[HotEntryListItemCount] = 50;
    Now, we're paying the storage cost for 'H' (1 byte) and the value.

    So -- we have converted all of our LDT lua code to follow this
    convention (fields become variables the reference a single char)
    and the mapping of long name to single char will be done in the code.

----------------------------------------------------------------------
## Control Maps

Names: for Property Maps and Control Maps

```
Note:  All variables that are field names will be upper case.
It is EXTREMELY IMPORTANT that these field names ALL have unique char
values -- within any given map.  They do NOT have to be unique across
the maps (and there's no need -- they serve different purposes).
Note that we've tried to make the mapping somewhat cannonical where
possible. 
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Record Level Property Map (RPM) Fields: One RPM per record
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local RPM_LdtCount             = 'C';  -- Number of LDTs in this rec
local RPM_VInfo                = 'V';  -- Partition Version Info
local RPM_Magic                = 'Z';  -- Special Sauce
local RPM_SelfDigest           = 'D';  -- Digest of this record
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
LDT specific Property Map (PM) Fields: One PM per LDT bin:
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
Note: The TopRec keeps this in the single LDT Bin (RPM).
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Lso Data Record (LDR) Control Map Fields (Recall that each Map ALSO has
the PM (general property map) fields.
local LDR_StoreMode            = 'M'; !! Use Top LSO Entry
local LDR_ListEntryMax         = 'L'; !! Use top LSO entry
local LDR_ByteEntrySize        = 'e'; !! Use Top LSO Entry
local LDR_ByteEntryCount       = 'C'; -- Current Count of bytes used
local LDR_ByteCountMax         = 'X'; !! Use Top LSO Entry
local LDR_LogInfo              = 'I'; !! Not currently used
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Cold Directory Control Map::In addition to the General Property Map
local CDM_NextDirRec           = 'N';-- Ptr to next Cold Dir Page
local CDM_PrevDirRec           = 'P';-- Ptr to Prev Cold Dir Page
local CDM_DigestCount          = 'C';-- Current Digest Count
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Main LSTACK Map Field Name Mapping
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local M_StoreMode              = 'M'; -- List or Binary Mode
local M_StoreLimit             = 'S'; -- Max Item Count for stack
local M_UserModule             = 'P'; -- Name of the User Module
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
Note that WarmTopXXXXCount will eventually replace the need to show if
the Warm Top is FULL -- because we'll always know the count (and "full"
will be self-evident).
local M_WarmTopFull            = 'F'; -- Boolean: Shows if Warm Top is full
local M_WarmTopEntryCount      = 'A'; -- # of Objects in the Warm Top (LDR)
local M_WarmTopByteCount       = 'a'; -- # Bytes in the Warm Top (LDR)

Note that ColdTopListCount will eventually replace the need to know if
the Cold Top is FULL -- because we'll always know the count of the Cold
Directory Top -- and so "full" will be self-evident.
local M_ColdTopFull            = 'f'; -- Boolean: Shows if Cold Top is full
local M_ColdTopListCount       = 'T'; -- Shows List Count for Cold Top

local M_ColdDirListHead        = 'Z'; -- Digest of the Head of the Cold List
local M_ColdDirListTail        = 'z'; -- Digest of the Head of the Cold List
local M_ColdDataRecCount       = 'R';-- # of LDRs in Cold Storage
It's assumed that this will match the warm list size, and we'll move
half of the warm digest list to a cold list on each transfer.
local M_ColdListMax            = 'c';-- Max # of items in a cold dir list
This is used to LIMIT the size of an LSTACK -- we will do it efficiently
at the COLD DIR LEVEL.  So, for Example, if we set it to 3, then we'll
discard the last (full) cold Dir List when we add a new fourth Dir Head.
Thus, the number of FULL Cold Directory Pages "D" should be set at
(D + 1).
local M_ColdDirRecMax          = 'C';-- Max # of Cold Dir subrecs we'll have
local M_ColdDirRecCount        = 'r';-- # of Cold Dir sub-Records

```

## Control Map Field Naming
```

Maintain the LSTACK letter Mapping here, so that we never have a name
collision: Obviously -- only one name can be associated with a character.
We won't need to do this for the smaller maps, as we can see by simple
inspection that we haven't reused a character.

A:M_WarmTopEntryCount      a:M_WarmTopByteCount      0:
B:                         b:M_LdrByteCountMax       1:
C:M_ColdDirRecMax          c:M_ColdListMax           2:
D:                         d:                        3:
E:                         e:M_LdrEntryCountMax      4:
F:M_WarmTopFull            f:M_ColdTopFull           5:
G:                         g:                        6:
H:M_HotEntryList           h:M_HotListMax            7:
I:                         i:                        8:
J:                         j:                        9:
K:                         k:                  
L:M_HotEntryListItemCount  l:M_WarmListDigestCount
M:M_StoreMode              m:
N:                         n:
O:                         o:
P:M_UserModule             p:
Q:                         q:
R:M_ColdDataRecCount       r:M_ColdDirRecCount
S:M_StoreLimit             s:M_LdrByteEntrySize
T:M_ColdTopListCount       t:M_Transform
U:                         u:M_UnTransform
V:                         v:
W:M_WarmDigestList         w:M_WarmListMax
X:M_HotListTransfer        x:M_WarmListTransfer
Y:                         y:
Z:M_ColdDirListHead        z:M_ColdDirListTail
```

