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

local bit      = utils.bit
local Iter     = utils.Iter
local Buffer   = utils.Buffer
local count    = utils.count
local unpack   = utils.unpack

---------------------------------------------------------------------
local Message = {} do
Message.__index = Message

function Message:new(id, header, content)
  return setmetatable({
    _id      = id,
    _header  = header,
    _content = content,
  }, self)
end

function Message:send(peer, s)
  local header = Buffer():write(">c0BBI2",
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

  local ok, err = s:send_more(header)
  if not ok then return nil, err end

  if type(self._content) == "string" then
    return s:send(self._content)
  end

  assert(type(self._content) == "table")
  return s:send_all(self._content)
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local MessageEncoder = {} do

function MessageEncoder.HELLO(node)
  local buf = Buffer()
    :write_string(node:endpoint())
    :write_uint32(count(node:groups()))

  for name in pairs(node:groups()) do
    buf:write_longstr(name)
  end

  buf
    :write_uint8(node:status())
    :write_string(node:name())
    :write_uint32(count(node:headers()))

  for k, v in pairs(node:headers()) do
    buf
      :write_string(k)
      :write_longstr(v)
  end

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

function MessageEncoder.SHOUT(node, group, content)
  local buf = Buffer()
    :write_string(group)

  return Message:new(ZRE.COMMANDS.SHOUT, buf:data(), content)
end

function MessageEncoder.WHISPER(node, content)
  return Message:new(ZRE.COMMANDS.WHISPER, nil, content)
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local MessageProcessor = {} do

function MessageProcessor.HELLO(node, version, uuid, sequence, endpoint, groups, status, name, headers)
  local log  = node:logger()
  local peer = assert(node:require_peer(uuid, endpoint))

  if peer:ready() then
    log.alert("Get duplicate HELLO messge from ", peer:uuid(true), " ", name, " ", endpoint)
    node:remove_peer(peer):disconnect()
    return
  end

  if sequence ~= 1 then return end

  peer
    :set_status(status)
    :set_name(name)

  for key, val in pairs(headers) do
    peer:set_header(key, val)
  end

  -- Tell the caller about the peer
  node:send("ENTER", peer:uuid(true), peer:name(), 
    "", -- peer:headers() -- @todo pack headers hash
    peer:endpoint()
  )

  for group_name in pairs(groups) do
    node:join_peer_group(peer, group_name)
  end

  log.info("New peer ready ", peer:uuid(true), " ", name, " ", endpoint)

  peer:set_ready(true)
end

local function find_peer(node, version, uuid, sequence)
  local log  = node:logger()
  local peer = node:find_peer(uuid)
  if not peer then
    log.warning("Unknown peer: ", UUID.to_string(uuid))
    return
  end

  if peer:next_want_sequence() ~= sequence then
    node:remove_peer(peer):disconnect()
    log.warning("Invalid message sequence. Expected ", peer:next_want_sequence(0), " Got:", sequence)
    return
  end

  return peer
end

function MessageProcessor.PING(node, version, uuid, sequence)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  peer:send(MessageEncoder.PING_OK(node))
end

function MessageProcessor.PING_OK(node, version, uuid, sequence)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end
end

function MessageProcessor.JOIN(node, version, uuid, sequence, group, status)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:join_peer_group(peer, group)
end

function MessageProcessor.LEAVE(node, version, uuid, sequence, group, status)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:leave_peer_group(peer, group)
end

function MessageProcessor.SHOUT(node, version, uuid, sequence, group, content)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:send("SHOUT", peer:uuid(true), peer:name(), group, unpack(content))
end

function MessageProcessor.WHISPER(node, version, uuid, sequence, group, content)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:send("WHISPER", peer:uuid(true), peer:name(), unpack(content))
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local MessageDecoder = {} do

function MessageDecoder.HELLO(node, version, uuid, sequence, iter)
  local endpoint, list_size = iter:next(">Bc0I4")
  if not endpoint then return end

  local groups = {}
  for i = 1, list_size do
    local elem = iter:next_longstr()
    if not elem then return end
    groups[elem] = true
  end

  local status, name, hash_size = iter:next(">BBc0I4")
  if not status then return end
  
  local headers = {}
  for i = 1, hash_size do
    local key, value = iter:next_string(">Bc0I4c0")
    if not key then return end
    headers[key] = value
  end

  return MessageProcessor.HELLO(node, version, uuid, sequence, endpoint, groups, status, name, headers)
end

function MessageDecoder.PING(node, version, uuid, sequence)
  return MessageProcessor.PING(node, version, uuid, sequence)
end

function MessageDecoder.PING_OK(node, version, uuid, sequence)
  return MessageProcessor.PING_OK(node, version, uuid, sequence)
end

function MessageDecoder.JOIN(node, version, uuid, sequence, iter)
  local group, status = iter:next("Bc0B") if not group then return end

  return MessageProcessor.JOIN(node, version, uuid, sequence, group, status)
end

function MessageDecoder.LEAVE(node, version, uuid, sequence, iter)
  local group, status = iter:next("Bc0B") if not group then return end
  if not group then return end

  return MessageProcessor.LEAVE(node, version, uuid, sequence, group, status)
end

function MessageDecoder.SHOUT(node, version, uuid, sequence, iter, content)
  local group = iter:next_string() if not group then return end

  return MessageProcessor.SHOUT(node, version, uuid, sequence, group, content)
end

function MessageDecoder.WHISPER(node, version, uuid, sequence, iter, content)
  return MessageProcessor.WHISPER(node, version, uuid, sequence, content)
end

end
---------------------------------------------------------------------

local Message = {}

Message.encoder   = MessageEncoder
Message.decoder   = MessageDecoder
Message.processor = MessageProcessor

return Message
