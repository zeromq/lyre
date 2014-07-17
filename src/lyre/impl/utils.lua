local bit    = require "bit32"
local struct = require "struct"

local unpack = unpack or table.unpack

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
}
