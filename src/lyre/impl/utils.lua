--
--  Author: Alexey Melnichuk <mimir@newmail.ru>
--
--  Copyright (C) 2014 Alexey Melnichuk <mimir@newmail.ru>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of Lyre library.
--

local bit repeat
  local ok
  ok, bit = pcall(require, "bit32")
  if ok then break end
  bit = require "bit"
until true

local struct = require "struct"

local unpack = unpack or table.unpack

local BIG_ENDIAN = ">"
local UINT8      = "B"
local UINT16     = "I2"
local UINT32     = "I4"
local STRING     = "Bc0"
local LONGSTR    = "I4c0"
local BYTES      = function(N) return "c" .. tostring(N) end
local STRUCT     = table.concat

local HASH_ELEMENT = STRUCT{BIG_ENDIAN; STRING; LONGSTR}

local function count(t)
  local n = 0
  for _ in pairs(t) do n = n + 1 end
  return n
end

---------------------------------------------------------------------
local Iter = {} do
Iter.__index = Iter

function Iter:new(data)
  return setmetatable({_data = data, _pos = 1, _len = #data}, self)
end

local function wrap_next(self, ok, ...)
  if not ok then return end
  local pos = select("#", ...)
  self._pos = select(pos, ...)
  return ...
end

local function wrap_peek(self, ok, ...)
  if not ok then return end
  return ...
end

function Iter:next(fmt)
  return wrap_next(self, pcall(struct.unpack, fmt, self._data, self._pos))
end

function Iter:peek(fmt)
  return wrap_peek(self, pcall(struct.unpack, fmt, self._data, self._pos))
end

function Iter:has(n)
  return (self._len - self._pos + 1) >= n
end

function Iter:next_uint8()
  if not self:has(1) then return end

  local val = struct.unpack("B", self._data, self._pos)
  self._pos = self._pos + 1
  return val
end

function Iter:next_uint16()
  if not self:has(2) then return end

  local val = struct.unpack(">I2", self._data, self._pos)
  self._pos = self._pos + 2
  return val
end

function Iter:next_uint32()
  if not self:has(4) then return end

  local val = struct.unpack(">I4", self._data, self._pos)
  self._pos = self._pos + 4
  return val
end

function Iter:next_bytes(n)
  if not self:has(n) then return end

  local val = self._data:sub(self._pos, self._pos + n - 1)
  self._pos = self._pos + n
  return val
end

function Iter:next_string()
  return self:next("Bc0")
end

function Iter:next_longstr()
  return self:next(">I4c0")
end

function Iter:next_hash(t)
  t = t or {}
  local n = self:next_uint32() if not n then return end
  for i = 1, n do
    local k, v = self:next(HASH_ELEMENT) if not k then return end
    t[k] = v
  end
  return t
end

function Iter:next_set(t)
  t = t or {}
  local n = self:next_uint32() if not n then return end
  for i = 1, n do
    local k = self:next_longstr() if not k then return end
    t[k] = true
  end
  return t
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Buffer = {} do
Buffer.__index = Buffer

function Buffer:new()
  return setmetatable({_data = {}}, self)
end

function Buffer:write_bytes(v)
  self._data[#self._data + 1] = v
  return self
end

function Buffer:write_uint8(v)
  self._data[#self._data + 1] = struct.pack("B", v)
  return self
end

function Buffer:write_uint16(v)
  self._data[#self._data + 1] = struct.pack(">I2", v)
  return self
end

function Buffer:write_uint32(v)
  self._data[#self._data + 1] = struct.pack(">I4", v)
  return self
end

function Buffer:write_string(v)
  self._data[#self._data + 1] = struct.pack("Bc0", #v, v)
  return self
end

function Buffer:write_longstr(v)
  self._data[#self._data + 1] = struct.pack(">I4c0", #v, v)
  return self
end

function Buffer:write_hash(t)
  self:write_uint32(count(t))
  for k, v in pairs(t) do
    self:write(HASH_ELEMENT, #k, k, #v, v)
  end
  return self
end

function Buffer:write_set(t)
  self:write_uint32(count(t))
  for k in pairs(t) do self:write_longstr(k) end
  return self
end

function Buffer:write(...)
  self._data[#self._data + 1] = struct.pack(...)
  return self
end

function Buffer:data()
  return table.concat(self._data)
end

end
---------------------------------------------------------------------

return{
  Iter   = function(...) return Iter:new(...)   end;
  Buffer = function(...) return Buffer:new(...) end;
  count  = count;
  unpack = unpack;
  bit    = bit;

  STRUCT     = STRUCT;
  BIG_ENDIAN = BIG_ENDIAN;
  UINT8      = UINT8;
  UINT16     = UINT16;
  UINT32     = UINT32;
  STRING     = STRING;
  LONGSTR    = LONGSTR;
  BYTES      = BYTES;
}
