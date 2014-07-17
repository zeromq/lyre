--
--  Author: Alexey Melnichuk <mimir@newmail.ru>
--
--  Copyright (C) 2014 Alexey Melnichuk <mimir@newmail.ru>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of Lyre library.
--

local uuid = require "uuid"

local function StrUuidToBin(u)
  assert(#u == 36)
  return (string.gsub(u, "([^-][^-])%-?",function(ch)
    return string.char(tonumber(ch, 16))
  end))
end

local function Hex(s)
  return (string.gsub(s, ".",function(ch)
    return string.format("%.2x",string.byte(ch))
  end))
end

local function BinUuidToStr(u)
  assert(#u == 16)
  return (string.gsub(u, "^(....)(..)(..)(..)(......)$", function(a,b,c,d,e)
    return Hex(a) .. "-" .. Hex(b) .. "-" .. Hex(c) .. "-" .. Hex(d) .. "-" .. Hex(e)
  end))
end

local function NewStrUuid()
  return uuid.new()
end

local function NewBinUuid()
  return StrUuidToBin(uuid.new())
end

local UUID = {} do
UUID.__index = UUID

function UUID:new(u)
  if not u then u = NewBinUuid() end

  local o = setmetatable({},self)
  o._bin = u
  o._str = BinUuidToStr(u)

  return o
end

function UUID:bin()
  return self._bin
end

function UUID:str()
  return self._str
end

end

return {
  LEN       = 16;
  to_string = BinUuidToStr;
  to_binary = StrUuidToBin;
  new       = function(...) return UUID:new(...) end;
}
