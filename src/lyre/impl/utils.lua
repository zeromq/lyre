local bit = require "bit32"

local unpack = unpack or table.unpack

local function count(t)
  local n = 0
  for _ in pairs(t) do n = n + 1 end
  return n
end

---------------------------------------------------------------------
local Iter = {} do
Iter.__index = Iter

-- Get a 1-byte number from the frame
local function GET_NUMBER1(data, pos)
  return data:sub(pos, pos):byte(), pos + 1
end

-- Get a 2-byte number from the frame
local function GET_NUMBER2(data, pos)
  return bit.bor(
    bit.lshift(data:sub(pos + 0, pos + 0):byte(),  8),
               data:sub(pos + 1, pos + 1):byte()
  ), pos + 2
end

-- Get a 4-byte number from the frame
local function GET_NUMBER4(data, pos)
  return bit.bor(
    bit.lshift(data:sub(pos + 0, pos + 0):byte(), 24),
    bit.lshift(data:sub(pos + 1, pos + 1):byte(), 16),
    bit.lshift(data:sub(pos + 2, pos + 2):byte(),  8),
               data:sub(pos + 3, pos + 3):byte()
  ), pos + 4
end

-- Get a sequence of bytes
local function GET_BYTES(data, len, pos)
  local s = (data:sub(pos, pos + len - 1))
  return s, pos + len
end

function Iter:new(data)
  return setmetatable({_data = data, _pos = 1, _len = #data}, self)
end

function Iter:has(n)
  return (self._len - self._pos + 1) >= n
end

function Iter:next_uint8()
  if not self:has(1) then return end

  local val
  val, self._pos = GET_NUMBER1(self._data, self._pos)
  return val
end

function Iter:next_uint16()
  if not self:has(2) then return end

  local val
  val, self._pos = GET_NUMBER2(self._data, self._pos)
  return val
end

function Iter:next_uint32()
  if not self:has(4) then return end

  local val
  val, self._pos = GET_NUMBER4(self._data, self._pos)
  return val
end

function Iter:next_bytes(n)
  if not self:has(n) then return end

  local val
  val, self._pos = GET_BYTES(self._data, n, self._pos)
  return val
end

function Iter:next_string()
  local len = self:next_uint8()
  if not len then return end
  return self:next_bytes(len)
end

function Iter:next_longstr()
  local len = self:next_uint32()
  if not len then return end
  return self:next_bytes(len)
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
  self._data[#self._data + 1] = string.char(bit.band(v, 0xFF))
  return self
end

function Buffer:write_uint16(v)
  self._data[#self._data + 1] = 
    string.char(bit.band(bit.rshift(v, 8), 0xFF)) .. 
    string.char(bit.band(           v,     0xFF))
  return self
end

function Buffer:write_uint32(v)
  self._data[#self._data + 1] = 
    string.char(bit.band(bit.rshift(v, 24), 0xFF)) .. 
    string.char(bit.band(bit.rshift(v, 16), 0xFF)) .. 
    string.char(bit.band(bit.rshift(v, 8 ), 0xFF)) .. 
    string.char(bit.band(           v,      0xFF))
  return self
end

function Buffer:write_string(val)
  self:write_uint8(#val)
  self:write_bytes(val)
  return self
end

function Buffer:write_longstr(val)
  self:write_uint32(#val)
  self:write_bytes(val)
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
}
