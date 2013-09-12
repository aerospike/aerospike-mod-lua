-- ===================
-- Standard LDT ERRORS
-- ===================

-- Remember when we were here last
local MOD="2013_09_05.a";

-- These errors align with the errors found in:
-- client/aerospike/src/include/aerospike/as_status.h
-- as_status.h::AEROSPIKE_ERR_LDT_INTERNAL == ldt_errors.lua::ERR_INTERNAL
local exports = {

ERR_INTERNAL             ="1400:LDT-Internal Error",
ERR_NOT_FOUND            ="1401:LDT-Item Not Found",
ERR_UNIQUE_KEY           ="1402:LDT-Unique Key or Value Violation",
ERR_INSERT               ="1403:LDT-Insert Error",
ERR_SEARCH               ="1404:LDT-Search Error",
ERR_DELETE               ="1405:LDT-Delete Error",
ERR_TRANS_FUN_NOT_FOUND  ="1406:LDT-Key Function Not Found",
ERR_UNTRANS_FUN_NOT_FOUND="1407:LDT-Transform Function Not Found",
ERR_KEY_FUN_NOT_FOUND    ="1408:LDT-UN-Transform Function Not Found",
ERR_INPUT_PARM           ="1409:LDT-Input Parameter Error",

ERR_TYPE_MISMATCH        ="1410:LDT-Type Mismatch for LDT Bin",
ERR_NULL_BIN_NAME        ="1411:LDT-Null Bin Name",
ERR_BIN_NAME_NOT_STRING  ="1412:LDT-Bin Name Not a String",
ERR_BIN_NAME_TOO_LONG    ="1413:LDT-Bin Name Exceeds 14 char",
ERR_TOO_MANY_OPEN_SUBRECS="1414:LDT-Exceeded Open Sub-Record Limit",
ERR_TOP_REC_NOT_FOUND    ="1415:LDT-Top Record Not Found",
ERR_SUB_REC_NOT_FOUND    ="1416:LDT-Sub Record Not Found",
ERR_BIN_DOES_NOT_EXIST   ="1417:LDT-LDT Bin Does Not Exist",
ERR_BIN_ALREADY_EXISTS   ="1418:LDT-LDT Bin Already Exists",
ERR_BIN_DAMAGED          ="1419:LDT-LDT Bin is Damaged",

ERR_SUBREC_POOL_DAMAGED  ="1420:LDT-Sub Record Pool is Damaged",
ERR_SUBREC_DAMAGED       ="1421:LDT-Sub Record is Damaged",
ERR_SUBREC_OPEN          ="1422:LDT-Sub Record Open Error",
ERR_SUBREC_UPDATE        ="1423:LDT-Sub Record Update Error",
ERR_SUBREC_CREATE        ="1424:LDT-Sub Record Create Error",
ERR_SUBREC_DELETE        ="1425:LDT-Sub Record Delete Error",
ERR_SUBREC_CLOSE         ="1426:LDT-Sub Record Close Error",
ERR_CAPACITY_EXCEEDED    ="1427:LDT-Capacity Exceeded"
}

return exports;

-- ldt_errors.lua
--
-- Use:  
-- local ldte = require('ldt_errors')
--
-- Use the error constant in the error() function.
-- error( ldte.ERR_INTERNAL );

