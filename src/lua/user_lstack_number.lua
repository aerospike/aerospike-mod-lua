-- Example User Module for Large Stack (lstack)
-- This instance describes Compress/Uncompress Functions for reducing
-- 8 byte lua numbers into 2 byte values.

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- ======================================================================
local GP=true; -- Leave this set to true.
local F=true; -- Set F (flag) to true to turn ON global print

local ldt_settings=require('settings_lset');

-- Track the latest update data and iteration
local MOD="user_lstack_number:2013_09_20.a";

-- Define the "exports" table that contains all of the functions that we
-- want to make visible to the LDT UDF Code.  In this table we may define
-- additional functions, such as filters, transformations or untransformations.
local exports = {}

  -- ======================================================================
  -- Function compressNumber: Compress an 8 byte Lua number into a 2 byte
  -- number.  We can do this because we know the values will be less than 
  -- 2^16 (64k).
  -- Parms:
  -- (1) numberObject:
  -- (2) arglist (args ignored in this function)
  -- Return: the two byte (compressed) byte object.
  -- ======================================================================
  function exports.compressNumber( numberObject, arglist )
    local meth = "compressNumber()";
    GP=F and trace("[ENTER]: <%s:%s> numberObject(%s) ArgList(%s) \n",
      MOD, meth, tostring(numberObject), tostring(arglist));

    local b2 = bytes(2);
    bytes.put_int16(b2, 1,  numberObject ); -- 2 byte int

    GP=F and trace("[EXIT]: <%s:%s> Result(%s) \n", MOD, meth, tostring(b2));
    return b2;
  end -- compressNumber()

  -- ======================================================================
  -- Function unCompressNumber:  Restore a Lua number from a compressed
  -- 2 byte value.
  -- Parms:
  -- (1) b2: 2 byte number
  -- (2) arglist (args ignored in this function)
  -- Return: the regular Lua Number
  -- ======================================================================
  -- ======================================================================
  function exports.unCompressNumber( b2, arglist )
    local meth = "unCompressNumber()";
    GP=F and trace("[ENTER]: <%s:%s> packedB16(%s) ArgList(%s) \n",
                  MOD, meth, tostring(b16), tostring(arglist));

    local numberObject = bytes.get_int16(b2, 1 ); -- 2 byte int

    GP=F and trace("[EXIT]<%s:%s>Result(%s)",MOD,meth,tostring(numberObject));
    return numberObject;
  end -- unCompressNumber()

  -- ======================================================================
  -- Define the "adjust_settings" function (this is a reserved word for the
  -- LDT code) that will be invoked to change any standard settings.
  -- Notice that if we define any transformation/untransformation functions
  -- in this section, then we must ALSO call an LDT Override function to
  -- register the name (otherwise it won't get called).
  -- ======================================================================
  function exports.adjust_settings( ldtMap )
    local meth = "adjust_settings()";
    GP=F and trace("[ENTER]<%s:%s> ldtMap(%s)", MOD, meth, tostring(ldtMap));

    ldt_settings.use_package( ldtMap, "TestModeNumber" );

    ldt_settings.set_transform( ldtMap, "compressNumber" );
    ldt_settings.set_untransform( ldtMap, "unCompressNumber" );

    GP=F and trace("[EXIT]<%s:%s> ldtMap(%s)", MOD, meth, tostring(ldtMap));
  end

-- Must return the "exports" table so that it is visible to the LDT UDF code.
return exports;

-- All Done!!
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
