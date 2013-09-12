-- Example User Module for Large Map (lmap)
-- This instance just calls the standard package

local ldt_settings=require('settings_lmap');

-- Define the "exports" table that contains all of the functions that we
-- want to make visible to the LDT UDF Code.  In this table we may define
-- additional functions, such as filters, transformations or untransformations.
local exports = {}

  -- Define the "adjust_settings" function (this is a reserved word for the
  -- LDT code) that will be invoked to change any standard settings.
  -- Notice that if we define any transformation/untransformation functions
  -- in this section, then we must ALSO call an LDT Override function to
  -- register the name (otherwise it won't get called).
  function exports.adjust_settings( ldtMap )
    info("ENTERING USER ADJUST SETTINGS");
    ldt_settings.use_package( ldtMap, "StandardList" );
    info("LEAVING USER ADJUST SETTINGS");
  end

-- Must return the "exports" table so that it is visible to the LDT UDF code.
return exports;

-- All Done!!
