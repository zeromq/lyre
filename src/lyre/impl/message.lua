--
--  Author: Alexey Melnichuk <mimir@newmail.ru>
--
--  Copyright (C) 2014 Alexey Melnichuk <mimir@newmail.ru>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of Lyre library.
--

local UUID     = require "lyre.impl.uuid"
local utils    = require "lyre.impl.utils"
local ZRE      = require "lyre.zre"

local bit        = utils.bit
local Iter       = utils.Iter
local Buffer     = utils.Buffer
local count      = utils.count

local STRUCT, BIG_ENDIAN, BYTES  = utils.STRUCT, utils.BIG_ENDIAN, utils.BYTES
local UINT8,  UINT16,     UINT32 = utils.UINT8,  utils.UINT16,     utils.UINT32
local STRING, LONGSTR            = utils.STRING, utils.STRING

local BEACON_HEADER = STRUCT{
  BIG_ENDIAN;
  BYTES(4);  -- signature
  BYTES(16); -- version
  UINT16;    -- port
}

local MESSAGE_HEADER = STRUCT{
  BIG_ENDIAN;
  BYTES(2);  -- signature
  UINT8;     -- command
  UINT8;     -- version
  UINT16;    -- sequence
}

local HELLO_HEADER_1 = STRUCT{BIG_ENDIAN, STRING, UINT32}
local HELLO_HEADER_2 = STRUCT{BIG_ENDIAN, UINT8, STRING, UINT32}
local HASH_ELEMENT   = STRUCT{BIG_ENDIAN, STRING, LONGSTR}
local JOIN_HEADER    = STRUCT{BIG_ENDIAN, STRING, UINT8}
local LEAVE_HEADER   = STRUCT{BIG_ENDIAN, STRING, UINT8}

---------------------------------------------------------------------
local Message = {} do
Message.__index = Message

function Message:new(id, header, ...)
  local content = ...
  if select("#", ...) > 1 then content = {...} end

  return setmetatable({
    _id      = id,
    _header  = header,
    _content = content,
  }, self)
end

function Message:send(peer, s)
  local header = Buffer():write(MESSAGE_HEADER,
    ZRE.SIGNATURE, self._id,
    peer:version(),
    peer:next_sent_sequence()
  )

  if self._header then
    header:write_bytes(self._header)
  end

  header = header:data()

  if not self._content then
    return s:send(header)
  end

  if type(self._content) == "string" then
    return s:sendx(header, self._content)
  end

  assert(type(self._content) == "table")

  local ok, err = s:send_more(header)
  if not ok then return nil, err end
  return s:send_all(self._content)
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local MessageEncoder = {} do

function MessageEncoder.beacon(node, port)
  return Buffer():write(BEACON_HEADER, 
    ZRE.BEACON_PREFIX, node:uuid(), port
  ):data()
end

function MessageEncoder.HELLO(node)
  local buf = Buffer()
    :write_string(node:endpoint())
    :write_set(node:groups())
    :write_uint8(node:status())
    :write_string(node:name())
    :write_hash(node:headers())

  return Message:new(ZRE.COMMANDS.HELLO, buf:data())
end

function MessageEncoder.PING(node)
  return Message:new(ZRE.COMMANDS.PING)
end

function MessageEncoder.PING_OK(node)
  return Message:new(ZRE.COMMANDS.PING_OK)
end

function MessageEncoder.JOIN(node, group)
  local buf = Buffer()
    :write_string(group)
    :write_uint8(node:status())

  return Message:new(ZRE.COMMANDS.JOIN, buf:data())
end

function MessageEncoder.LEAVE(node, group)
  local buf = Buffer()
    :write_string(group)
    :write_uint8(node:status())

  return Message:new(ZRE.COMMANDS.LEAVE, buf:data())
end

function MessageEncoder.SHOUT(node, group, ...)
  local buf = Buffer()
    :write_string(group)

  return Message:new(ZRE.COMMANDS.SHOUT, buf:data(), ...)
end

function MessageEncoder.WHISPER(node, ...)
  return Message:new(ZRE.COMMANDS.WHISPER, nil, ...)
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local MessageDecoder = {} do

function MessageDecoder.dispatch(node, routing_id, msg, ...)
  local log = node:logger()

  if not routing_id                then return end
  if not msg                       then return end
  if #routing_id ~= UUID.LEN + 1   then return end
  if routing_id:sub(1,1) ~= '\001' then return end

  local uuid = routing_id:sub(2)

  local iter = Iter(msg)
  local signature, cmd, version, sequence = iter:next(MESSAGE_HEADER)

  if not signature              then return end
  if signature ~= ZRE.SIGNATURE then return end
  if version   ~= ZRE.VERSION   then return end

  local name = ZRE.COMMANDS_NAME[cmd]
  if not name then
    log.alert("Unknown command ", cmd, " from ", UUID.to_string(uuid))
    return
  end

  log.notice("INBOX : ", UUID.to_string(uuid), " ", name, " #", sequence)

  local fn = MessageDecoder[name]
  if fn then fn(node, version, uuid, sequence, iter, ...) end
end

function MessageDecoder.beacon(node, host, ann)
  if #ann ~= ZRE.ANN_SIZE then return end

  local prefix, uuid, port = Iter(ann):next(BEACON_HEADER)
  assert(prefix == ZRE.BEACON_PREFIX) -- beacon filter out any wrong messages

  return node:on_message("beacon", ZRE.BEACON_VERSION, uuid, host, port)
end

function MessageDecoder.HELLO(node, version, uuid, sequence, iter)
  local endpoint, list_size = iter:next(HELLO_HEADER_1)
  if not endpoint then return end

  local groups = {}
  for i = 1, list_size do
    local elem = iter:next_longstr()
    if not elem then return end
    groups[elem] = true
  end

  local status, name, hash_size = iter:next(HELLO_HEADER_2)
  if not status then return end
  
  local headers = {}
  for i = 1, hash_size do
    local key, value = iter:next(HASH_ELEMENT)
    if not key then return end
    headers[key] = value
  end

  return node:on_message("HELLO", version, uuid, sequence, endpoint, groups, status, name, headers)
end

function MessageDecoder.PING(node, version, uuid, sequence)
  return node:on_message("PING", version, uuid, sequence)
end

function MessageDecoder.PING_OK(node, version, uuid, sequence)
  return node:on_message("PING_OK", version, uuid, sequence)
end

function MessageDecoder.JOIN(node, version, uuid, sequence, iter)
  local group, status = iter:next(JOIN_HEADER) if not group then return end

  return node:on_message("JOIN", version, uuid, sequence, group, status)
end

function MessageDecoder.LEAVE(node, version, uuid, sequence, iter)
  local group, status = iter:next(LEAVE_HEADER) if not group then return end
  if not group then return end

  return node:on_message("LEAVE", version, uuid, sequence, group, status)
end

function MessageDecoder.SHOUT(node, version, uuid, sequence, iter, ...)
  local group = iter:next_string() if not group then return end

  return node:on_message("SHOUT", version, uuid, sequence, group, ...)
end

function MessageDecoder.WHISPER(node, version, uuid, sequence, iter, ...)
  return node:on_message("WHISPER", version, uuid, sequence, ...)
end

end
---------------------------------------------------------------------

local Message = {}

Message.encoder = MessageEncoder
Message.decoder = MessageDecoder

return Message
