-- Example User Module for lstack
-- First -- get the settings file for this LDT (lstack)
-- ++===============++
-- || Package Names ||
-- ++===============++
-- Current valid package names:
-- Package Names for "pre-packaged" settings:
-- "StandardList";
-- "ProdListValBinStore";
-- "TestModeList";
-- "TestModeBinary";
-- "DebugModeObject";
-- "DebugModeObjectDups";
-- "DebugModeList";
-- "DebugModeBinary";

local lstack_settings=require('settings_lstack');

-- Next -- export the "adjust_settings" function
local exports = {}

  function exports.adjust_settings( ldtMap )
    info("ENTERING USER ADJUST SETTINGS");
    lstack_settings.use_package( ldtMap, "StandardList" );
    info("LEAVING USER ADJUST SETTINGS");
  end

return exports;

