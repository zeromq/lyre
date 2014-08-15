--
--  Author: Alexey Melnichuk <mimir@newmail.ru>
--
--  Copyright (C) 2014 Alexey Melnichuk <mimir@newmail.ru>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of Lyre library.
--

-- ZRE Constants
local ZRE_DISCOVERY_PORT = 5670
local ZRE_BEACON_VERSION = 1
local ZRE_BEACON_PREFIX  = "ZRE" .. string.char(ZRE_BEACON_VERSION)
local ZRE_VERSION        = 2
local ZRE_UUID_LEN       = 16
local ZRE_ANN_SIZE       = #ZRE_BEACON_PREFIX + ZRE_UUID_LEN + 2 -- UUID + PORT
local ZRE_SIGNATURE      = string.char(0xAA) .. string.char(0xA1)
local ZRE_COMMANDS       = {
  HELLO     = 1;
  WHISPER   = 2;
  SHOUT     = 3;
  JOIN      = 4;
  LEAVE     = 5;
  PING      = 6;
  PING_OK   = 7;
}
local ZRE_COMMANDS_NAME  = {} for k, v in pairs(ZRE_COMMANDS) do ZRE_COMMANDS_NAME[v] = k end

return {
  DISCOVERY_PORT = ZRE_DISCOVERY_PORT;
  BEACON_VERSION = ZRE_BEACON_VERSION;
  BEACON_PREFIX  = ZRE_BEACON_PREFIX;
  VERSION        = ZRE_VERSION;
  ANN_SIZE       = ZRE_ANN_SIZE;
  SIGNATURE      = ZRE_SIGNATURE;
  COMMANDS       = ZRE_COMMANDS;
  COMMANDS_NAME  = ZRE_COMMANDS_NAME;
}
