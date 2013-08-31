-- Example User Module for lstack
-- First -- get the settings file for this LDT (lstack)
-- ++===============++
-- || Package Names ||
-- ++===============++
-- Current valid package names:
-- Package Names for "pre-packaged" settings:
local PackageStandardList        = "StandardList";
local PackageProdListValBinStore = "ProdListValBinStore";
local PackageTestModeList        = "TestModeList";
local PackageTestModeBinary      = "TestModeBinary";
local PackageDebugModeObject     = "DebugModeObject";
local PackageDebugModeObjectDups = "DebugModeObjectDups";
local PackageDebugModeList       = "DebugModeList";
local PackageDebugModeBinary     = "DebugModeBinary";

local lstack_settings=require('settings_lstack');

-- Next -- export the "adjust_settings" function
local exports = {

  function exports.adjust_settings( ldtMap )
    info("ENTERING USER ADJUST SETTINGS");
    lstack_settings.use_package( ldtMap, PackageDebugModeList );
    info("LEAVING USER ADJUST SETTINGS");
  end

}

return exports;

