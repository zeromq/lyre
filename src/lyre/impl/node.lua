--
--  Author: Alexey Melnichuk <mimir@newmail.ru>
--
--  Copyright (C) 2014 Alexey Melnichuk <mimir@newmail.ru>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of Lyre library.
--

---------------------------------------------------------------------
--  Constants, to be configured/reviewed
local PEER_EVASIVE       = 3000   --  3 seconds' silence is evasive
local PEER_EXPIRED       = 5000   --  5 seconds' silence is expired
local REAP_INTERVAL      = 1000   --  Once per second
---------------------------------------------------------------------

local zmq      = require "lzmq"
local zloop    = require "lzmq.loop"
local ztimer   = require "lzmq.timer"
local zbeacon  = require "lzmq.beacon"
local zthreads = require "lzmq.threads"
local LogLib   = require "log"
local ZRE      = require "lyre.zre"
local Peer     = require "lyre.impl.peer"
local UUID     = require "lyre.impl.uuid"
local utils    = require "lyre.impl.utils"
local Group    = require "lyre.impl.group"
local Message  = require "lyre.impl.message"

local bit            = utils.bit
local Buffer         = utils.Buffer
local MessageDecoder = Message.decoder
local MessageEncoder = Message.encoder

---------------------------------------------------------------------
local Node_api_dispatch do

local Node_api = {}

function Node_api_dispatch(self, pipe, cmd, ...)
  local log = self:logger()
  if not cmd then
    log.error("Can not recv API command:", ...)
    return nil, ...
  end

  local fn = Node_api[cmd]
  if fn then
    log.debug("API start: ", cmd, ...)
    local ok, err = fn(self, pipe, ...)
    if not ok then
      log.error("API error: ", cmd, " - ", err)
    else
      log.debug("API done : ", cmd)
    end
    return ok, err
  end

  log.alert("Recv unknown API command: ", api, ...)

  return nil, 'Unknown command'
end

Node_api[ "SET NAME"     ] = function (self, pipe, name)
  self:set_name(name)
  return true
end

Node_api[ "SET HEADER"   ] = function (self, pipe, name, value)
  self:set_header(name, value)
  return true
end

Node_api[ "SET VERBOSE"  ] = function (self, pipe, level)
  if level then self:set_log_level(level) end
  return true
end

Node_api[ "SET LOG WRITER"] = function (self, pipe, writer)
  if not writer then return end
  local loadstring = loadstring or load
  writer = loadstring(writer) if not writer then return end
  writer = writer()           if not writer then return end
  self:set_log_writer(writer)
  return true
end

Node_api[ "SET PORT"     ] = function (self, pipe, value)
  value = tonumber(value)
  if value then self:set_beacon_port(value) end
  return true
end

Node_api[ "SET HOST"     ] = function (self, pipe, value)
  if value then self:set_beacon_host(value) end
  return true
end

Node_api[ "SET INTERVAL" ] = function (self, pipe, value)
  value = tonumber(value)
  if value then self:set_beacon_interval(value) end
  return true
end

Node_api[ "UUID"         ] = function (self, pipe)
  return self:api_response(self:uuid(true))
end

Node_api[ "NAME"         ] = function (self, pipe)
  return self:api_response(self:name())
end

Node_api[ "ENDPOINT"     ] = function (self, pipe)
  return self:api_response(self:endpoint())
end

Node_api[ "BIND"         ] = function (self, pipe, endpoint)
  return self:api_response(self:bind(endpoint))
end

Node_api[ "CONNECT"      ] = function (self, pipe, endpoint)
  return self:api_response(self:connect(endpoint))
end

Node_api[ "START"        ] = function (self, pipe)
  return self:api_response(self:start())
end

Node_api[ "STOP"         ] = function (self, pipe)
  return self:api_response(self:stop())
end

Node_api[ "WHISPER"      ] = function (self, pipe, identity, ...)
  local ok, err = self:whisper(identity, ...)
  return true
end

Node_api[ "SHOUT"        ] = function (self, pipe, group, ...)
  local ok, err = self:shout(group, ...)
  return true
end

Node_api[ "JOIN"         ] = function (self, pipe, group)
  local ok, err = self:join(group)
  return true
end

Node_api[ "LEAVE"        ] = function (self, pipe, group)
  local ok, err = self:leave(group)
  return true
end

Node_api[ "$TERM"        ] = function (self, pipe)
  local ok, err = self:interrupt()
  return true
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Node_on_message = {} do

function Node_on_message.beacon(node, version, uuid, host, port)
  local log  = node:logger()
  if port > 0 then
    local endpoint = "tcp://" .. host .. ":" .. port
    local peer     = node:require_peer(uuid, endpoint)
    log.trace("BEACON: ", peer:uuid(true), peer:endpoint())
    return
  end

  local peer = self:find_peer(uuid)
  if peer then self:remove_peer(peer):disconnect() end
end

function Node_on_message.HELLO(node, version, uuid, sequence, endpoint, groups, status, name, headers)
  local log  = node:logger()

  if sequence ~= 1 then
    log.alert("Get HELLO with seq: ",sequence, " from", UUID.to_string(uuid), " ", name, " ", endpoint)
    peer = node:find_peer(uuid)
    if peer then
      node:remove_peer(peer):disconnect()
    end
    return
  end

  local peer, err = node:require_peer(uuid, endpoint)
  if not peer then
    log.alert("Can not create peer:", UUID.to_string(uuid), " ", name, " ", endpoint)
    return
  end

  if peer:ready() then
    log.alert("Get duplicate HELLO messge from ", peer:uuid(true), " ", name, " ", endpoint)
    node:remove_peer(peer):disconnect()
    return
  end

  peer
    :set_status(status)
    :set_name(name)

  for key, val in pairs(headers) do
    peer:set_header(key, val)
  end

  -- Tell the caller about the peer
  node:send("ENTER", peer:uuid(true), peer:name(), 
    Buffer():write_hash(peer:headers()):data(),
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

function Node_on_message.PING(node, version, uuid, sequence)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  peer:send(MessageEncoder.PING_OK(node))
end

function Node_on_message.PING_OK(node, version, uuid, sequence)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end
end

function Node_on_message.JOIN(node, version, uuid, sequence, group, status)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:join_peer_group(peer, group)
end

function Node_on_message.LEAVE(node, version, uuid, sequence, group, status)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:leave_peer_group(peer, group)
end

function Node_on_message.SHOUT(node, version, uuid, sequence, group, content)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:send("SHOUT", peer:uuid(true), peer:name(), group, unpack(content))
end

function Node_on_message.WHISPER(node, version, uuid, sequence, group, content)
  local peer = find_peer(node, version, uuid, sequence)
  if not peer then return end

  node:send("WHISPER", peer:uuid(true), peer:name(), unpack(content))
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Node = {} do
Node.__index = Node

local function Node_on_beacon(self, beacon)
  local host, ann = beacon:recv()
  if not host then return end
  return MessageDecoder.beacon(self, host, ann)
end

local function wrap_msg(a, b, ...)
  return a, b, {...}
end

local function Node_on_inbox(self, inbox)
  local routing_id, msg, content = wrap_msg(inbox:recvx())
  if not routing_id              then return end
  if not msg                     then return end

  return MessageDecoder.dispatch(self, routing_id, msg, content)
end

local function Node_on_interval(self)
  local log = self:logger()

  for id, peer in pairs(self._private.peers) do
    if peer:expire() then
      log.info(peer:name(), " ", peer:endpoint(), " - expire")
      self:remove_peer(peer):disconnect()
    end
  end

  local msg
  for id, peer in pairs(self._private.peers) do
    if peer:evasive() then
      msg = msg or MessageEncoder.PING(self)
      peer:send(msg)
    end
  end

end

local function Node_on_command(self, pipe)
  return Node_api_dispatch(self, pipe, pipe:recvx())
end

function Node:new(pipe, outbox)
  local ctx = zmq.assert(zthreads.context())

  outbox = zmq.assert(ctx:socket{zmq.PAIR, connect = outbox})

  local uuid = UUID.new()

  local LYRE_MYIP = LYRE_MYIP or os.getenv("LYRE_MYIP")

  local o = setmetatable({}, self)

  local loop = zloop.new(4)

  if pipe then loop:add_socket(pipe, function(s) Node_on_command(o, s) end) end

  o._private = {
    pipe     = pipe;         -- chanel to internal API
    inbox    = inbox;        -- other nodes connect to
    outbox   = outbox;       -- events to user API
    uuid     = uuid;         -- 
    name     = uuid:str():sub(1,6);
    loop     = loop;
    groups   = {};
    status   = 0;
    peer_groups = {};
    headers  = {};
    peers    = {};
    interval = REAP_INTERVAL or 0; -- beacon internal
    host     = LYRE_MYIP;          -- beacon host
    port     = ZRE.DISCOVERY_PORT; -- beacon port
    logger   = LogLib.new('none',
      function(...) return o._private.log_writer(...) end,
      o:_formatter(require "log.formatter.concat".new())
    );
    log_writer = require "log.writer.stdout".new();
  }

  return o
end

function Node:_formatter( fn )
  return function(...)
    return string.format("[%s] %s", self:name(), fn(...))
  end
end

function Node:bind(endpoint)
  local p = self._private
  assert(not p.endpoint)
  assert(not p.inbox)

  local inbox, err  = ctx:socket{zmq.ROUTER, router_handover = 1}
  if not inbox then return nil, err  end

  local port = tonumber(endpoint:match(":(%d+)$"))
  local ok, err = inbox:bind(endpoint)
  if not ok then
    inbox:close()
    return nil, err
  end

  p.inbox = inbox
  p.endpoint = endpoint
  return self
end

function Node:start()
  local p = self._private

  local ctx = p.loop:context()

  if not p.endpoint then
    local inbox, beacon, err

    local function local_cleanup()
      if inbox then inbox:close() end
      if beacon then beacon:destroy() end
    end
    
    inbox, err = ctx:socket{zmq.ROUTER, router_handover = 1}
    if not inbox then local_cleanup() return nil, err  end

    if self:beacon_host() then
      beacon, err = zbeacon.new(self:beacon_host(), self:beacon_port())
    else
      beacon, err = zbeacon.new(self:beacon_port())
    end

    if not beacon then local_cleanup() return nil, err end

    local host, port
    host, err = beacon:host()
    if not host then local_cleanup() return nil, err end

    port, err = inbox:bind_to_random_port("tcp://" .. host)
    if not port then local_cleanup() return nil, err end

    local endpoint = ("tcp://%s:%d"):format(host, port)
    local announcement = MessageEncoder.beacon(self, port)

    local ok
    if self._private.interval and self._private.interval > 0 then
      ok, err = beacon:interval(self._private.interval)
      if not ok then local_cleanup() return nil, err end
    end

    ok, err = beacon:noecho()                     if not ok then local_cleanup() return nil, err end
    ok, err = beacon:publish(announcement)        if not ok then local_cleanup() return nil, err end
    ok, err = beacon:subscribe(ZRE.BEACON_PREFIX) if not ok then local_cleanup() return nil, err end

    p.endpoint = endpoint
    p.inbox    = inbox
    p.beacon   = p.loop:add_socket(beacon, function(s) Node_on_beacon(self, s) end)
  end

  p.loop:add_socket(p.inbox, function(s) Node_on_inbox(self, s) end)

  p.loop:add_interval(REAP_INTERVAL, function() Node_on_interval(self) end)

  return self
end

function Node:stop()
  local p = self._private

  if p.beacon then
    p.beacon:publish(MessageEncoder.beacon(self, 0))
  end

  if p.loop then
    if p.inbox  then p.loop:remove_socket(p.inbox)  end
    if p.beacon then p.loop:remove_socket(p.beacon) end
  end

  for _, peer in pairs(self._private.peers) do
    self:remove_peer(peer):disconnect()
  end

  if p.inbox then
    p.inbox:close()
    p.inbox, p.endpoint = nil
  end

  if p.beacon then
    ztimer.sleep(1000)
    p.beacon:destroy()
  end

  return self
end

function Node:run()
  return self._private.loop:start()
end

function Node:on_message(msg, ...)
  Node_on_message[msg](self, ...)
end

function Node:destroy()
  local p = self._private
  self:stop()

  if p.loop then
    p.loop:destroy()
    p.loop = nil
  end

  if p.outbox then
    p.outbox:close()
    p.outbox = nil
  end
end

function Node:_loop()
  return self._private.loop
end

function Node:logger()
  return self._private.logger
end

function Node:set_log_level(lvl)
  self:logger().set_lvl(lvl)
  return self
end

function Node:set_log_writer(writer)
  self._private.log_writer = writer
  return self
end

function Node:status()
  return self._private.status
end

function Node:inc_status(i)
  local p = self._private
  p.status = bit.band(p.status + (i or 1), 0xFF)
  return p.status
end

function Node:name()
  return self._private.name
end

function Node:set_name(name)
  assert(type(name) == "string")
  self._private.name = assert(name)
  return self
end

function Node:uuid(str)
  if str then return self._private.uuid:str() end
  return self._private.uuid:bin()
end

function Node:endpoint()
  return self._private.endpoint
end

function Node:groups()
  return self._private.groups
end

function Node:join_peer_group(peer, name)
  local p = self._private
  local group = p.peer_groups[name] or Group.new(name)
  p.peer_groups[name] = group
  group:join(peer)

  self:send("JOIN", peer:uuid(true), peer:name(), name)

  return group
end

function Node:leave_peer_group(peer, name)
  local p = self._private
  local group = p.peer_groups[name]
  if not group then return true end
  group:leave(peer)

  self:send("JOIN", peer:uuid(true), peer:name(), name)

  return true
end

function Node:require_peer(uuid, endpoint)
  local log = self:logger()
  local p = self._private

  local peer = p.peers[uuid]
  if not peer then
    log.info("New peer detected: ", UUID.to_string(uuid), endpoint)
    for u, pp in pairs(p.peers) do
      if pp:endpoint() == endpoint then
        log.warning('Found peer with same endpoint:', pp:uuid(true), ". Remove it")
        self:remove_peer(pp):disconnect()
      end
    end

    local err

    peer, err = Peer.new(self, uuid)
    if not peer then
      print("Error:", err)
      return nil, err
    end

    p.peers[uuid] = peer
  end

  if not peer:connected() then
    local ok, err = peer:connect(self:uuid(), endpoint)
    if not ok then return nil, err end

    log.info("Send HELLO to ", peer:uuid(true), ' ', peer:endpoint())
    peer:send(MessageEncoder.HELLO(self))
  end

  peer:refresh()
  return peer
end

function Node:find_peer(uuid)
  local peer = self._private.peers[uuid]
  if peer then peer:refresh() end
  return peer
end

function Node:remove_peer(peer)
  local p = self._private
  for id, group in pairs(p.peer_groups) do
    group:leave(peer)
  end
  if p.peers[peer:uuid()] then
    self:send("EXIT", peer:uuid(true), peer:name())
    p.peers[peer:uuid()] = nil
  end
  return peer
end

function Node:headers()
  return self._private.headers
end

function Node:set_header(k, v)
  self._private.headers[k] = v
  return self
end

function Node:header(k)
  return self._private.headers[k]
end

function Node:set_beacon_interval(v)
  self._private.interval = v
  return self
end

function Node:beacon_interval(v)
  return self._private.interval
end

function Node:set_beacon_host(v)
  self._private.host = v
  return self
end

function Node:beacon_host(v)
  return self._private.host
end

function Node:set_beacon_port(v)
  self._private.port = v
  return self
end

function Node:beacon_port(v)
  return self._private.port
end

function Node:api_response(ok, err)
  if ok then return self._private.pipe:send("1") end
  return self._private.pipe:sendx("0", tostring(err))
end

function Node:send(...)
  return self._private.outbox:sendx(...)
end

function Node:shout(name, content)
  local p = self._private
  local group = p.peer_groups[name]
  if not group then return true end
  local msg = MessageEncoder.SHOUT(self, group:name(), content)
  return group:send(msg)
end

function Node:whisper(uuid, content)
  local p = self._private
  local peer = p.peers[uuid]
  if not peer then return true end
  local msg = MessageEncoder.WHISPER(self, content)
  return peer:send(msg)
end

function Node:join(name)
  local p = self._private

  if p.groups[name] then return true end

  p.groups[name] = true
  self:inc_status()

  local msg = MessageEncoder.JOIN(self, name)
  for _, peer in pairs(p.peers) do
    peer:send(msg)
  end

  return true
end

function Node:leave(name)
  local p = self._private

  if not p.groups[name] then return true end

  p.groups[name] = nil
  self:inc_status()

  local msg = MessageEncoder.LEAVE(self, name)
  for _, peer in pairs(p.peers) do
    peer:send(msg)
  end

  return true
end

end
---------------------------------------------------------------------

return {
  new = function(...) return Node:new(...) end;
}
