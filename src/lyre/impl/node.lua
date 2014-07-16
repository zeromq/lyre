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
local UUID     = require "lyre.impl.uuid"
local utils    = require "lyre.impl.utils"
local bit      = utils.bit
local Iter     = utils.Iter
local Buffer   = utils.Buffer
local count, pack = utils.count, utils.pack

-- ZRE Constants
local ZRE_DISCOVERY_PORT = 5670
local ZRE_BEACIN_VERSION = 1
local ZRE_VERSION        = 2
local ZRE_PREFIX         = "ZRE" .. string.char(ZRE_BEACIN_VERSION)
local ZRE_UUID_LEN       = UUID.LEN
local ZRE_ANN_SIZE       = #ZRE_PREFIX + ZRE_UUID_LEN + 2 -- UUID + PORT
local ZRE_SIGNATURE      = string.char(0xAA) .. string.char(0xA0)
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

---------------------------------------------------------------------
local MessageEncoder = {} do

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
  local header = Buffer()
    :write_bytes(ZRE_SIGNATURE)
    :write_uint8(self._id)
    :write_uint8(peer:version())
    :write_uint16(peer:next_sent_sequence())

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

  return Message:new(ZRE_COMMANDS.HELLO, buf:data())
end

function MessageEncoder.PING(node)
  return Message:new(ZRE_COMMANDS.PING)
end

function MessageEncoder.PING_OK(node)
  return Message:new(ZRE_COMMANDS.PING_OK)
end

function MessageEncoder.JOIN(node, group)
  local buf = Buffer()
    :write_string(group)
    :write_uint8(node:status())

  return Message:new(ZRE_COMMANDS.JOIN, buf:data())
end

function MessageEncoder.LEAVE(node, group)
  local buf = Buffer()
    :write_string(group)
    :write_uint8(node:status())

  return Message:new(ZRE_COMMANDS.LEAVE, buf:data())
end

function MessageEncoder.SHOUT(node, group, content)
  local buf = Buffer()
    :write_string(group)

  return Message:new(ZRE_COMMANDS.SHOUT, buf:data(), content)
end

function MessageEncoder.WHISPER(node, content)
  return Message:new(ZRE_COMMANDS.WHISPER, nil, content)
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

  -- Or we can force check 
  -- assert(sequence == 1)

  peer:set_want_sequence(sequence)
  peer:set_name(name)

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
  local endpoint  = iter:next_string() if not endpoint   then return end

  local list_size = iter:next_uint32() if not list_size  then return end
  local groups = {}
  for i = 1, list_size do
    local elem = iter:next_longstr()   if not elem       then return end
    groups[elem] = true
  end

  local status   = iter:next_uint8()   if not status     then return end
  local name     = iter:next_string()  if not name       then return end

  local hash_size = iter:next_uint32() if not hash_size  then return end
  local headers = {}
  for i = 1, hash_size do
    local key   = iter:next_string()   if not key        then return end
    local value = iter:next_longstr()  if not value      then return end
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
  local group  = iter:next_string() if not group  then return end
  local status = iter:next_uint8()  if not status then return end

  return MessageProcessor.JOIN(node, version, uuid, sequence, group, status)
end

function MessageDecoder.LEAVE(node, version, uuid, sequence, iter)
  local group  = iter:next_string() if not group  then return end
  local status = iter:next_uint8()  if not status then return end

  return MessageProcessor.LEAVE(node, version, uuid, sequence, group, status)
end

function MessageDecoder.SHOUT(node, version, uuid, sequence, iter, content)
  local group = iter:next_string() if not group  then return end

  return MessageProcessor.SHOUT(node, version, uuid, sequence, group, content)
end

function MessageDecoder.WHISPER(node, version, uuid, sequence, iter, content)
  return MessageProcessor.WHISPER(node, version, uuid, sequence, content)
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Group = {} do
Group.__index = Group

function Group:new(name)
  local o = setmetatable({}, self)
  o._private = {
    name  = name;
    peers = {};
  }
  return o
end

function Group:destroy()
end

function Group:name()
  return self._private.name
end

function Group:join(peer)
  self._private.peers[peer:uuid()] = peer
  peer:inc_status()
  return peer
end

function Group:leave(peer)
  self._private.peers[peer:uuid()] = nil
  peer:inc_status()
  return peer
end

function Group:send(msg)
  for _, peer in pairs(self._private.peers) do
    peer:send(msg)
  end
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Peer = {} do
Peer.__index = Peer

function Peer:new(uuid)
  local o = setmetatable({}, self)
  uuid = UUID.new(uuid)
  o._private = {
    uuid          = uuid;
    name          = uuid:str():sub(1, 6);
    version       = 2;
    sent_sequence = 0;
    want_sequence = 0;
    status        = 1;
    headers       = {};
  }
  return o
end

function Peer:connect(node_uuid, endpoint)
  assert(not self:connected())
  local ctx = zmq.assert(zthreads.context())

  local mailbox, err = ctx:socket{zmq.DEALER,
    identity = '\001' .. node_uuid;
    sndhwm   = PEER_EXPIRED * 100;
    sndtimeo = 0;
    connect  = endpoint;
  }
  if not mailbox then return nil, err end

  self._private.mailbox    = mailbox;
  self._private.endpoint   = endpoint;
  self._private.evasive_at = ztimer.monotonic():start(PEER_EVASIVE)
  self._private.expired_at = ztimer.monotonic():start(PEER_EXPIRED)

  return self
end

function Peer:refresh()
  self._private.evasive_at:start()
  self._private.expired_at:start()
end

function Peer:evasive()
  return self._private.evasive_at:rest() == 0
end

function Peer:expire()
  return self._private.evasive_at:rest() == 0
end

function Peer:send(msg)

  local ok, err = msg:send(self, self._private.mailbox)
  if not ok then
    if err:name() == 'EAGAIN' then
      self:disconnect()
      return nil, err
    end
    zmq.assert(nil, err)
  end

  return true
end

function Peer:endpoint()
  return self._private.endpoint or ""
end

function Peer:version()
  return self._private.version
end

function Peer:next_sent_sequence(i)
  local p = self._private
  p.sent_sequence = bit.band(p.sent_sequence + (i or 1), 0xFFFF)
  return p.sent_sequence
end

function Peer:next_want_sequence(i)
  local p = self._private
  p.want_sequence = bit.band(p.want_sequence + (i or 1), 0xFFFF)
  return p.want_sequence
end

function Peer:set_want_sequence(v)
  local p = self._private
  p.want_sequence = bit.band(v, 0xFFFF)
  return p.want_sequence
end

function Peer:status()
  return self._private.status
end

function Peer:inc_status(i)
  local p = self._private
  p.status = bit.band(p.status + (i or 1), 0xFF)
  return p.status
end

function Peer:ready()
  return not not self._private.ready
end

function Peer:set_ready(v)
  self._private.ready = v
end

function Peer:connected()
  return not not self._private.mailbox
end

function Peer:status()
  return self._private.status
end

function Peer:name()
  return self._private.name
end

function Peer:set_name(name)
  self._private.name = name
  return self
end

function Peer:uuid(as_str)
  if as_str then return self._private.uuid:str() end
  return self._private.uuid:bin()
end

function Peer:headers()
  return self._private.headers
end

function Peer:set_header(k, v)
  self._private.headers[k] = v
  return self
end

function Peer:header(k)
  return self._private.headers[k]
end

function Peer:disconnect()
  if self:connected() then
    local p = self._private
    p.mailbox:close()
    p.mailbox, p.uuid, p.endpoint, p.ready = nil
  end
end

end
---------------------------------------------------------------------

---------------------------------------------------------------------
local Node = {} do
Node.__index = Node

local function Node_on_beacon(self, beacon)
  local log  = self:logger()
  local host, ann = beacon:recv()

  if not host                               then return end
  if #ann ~= ZRE_ANN_SIZE                   then return end

  local iter = Iter(ann)
  if iter:next_bytes(#ZRE_PREFIX ) ~= ZRE_PREFIX then return end
  local uuid = iter:next_bytes(ZRE_UUID_LEN)
  local port = iter:next_uint16()

  if port > 0 then
    local endpoint = "tcp://" .. host .. ":" .. port

    log.trace("BEACON: ", UUID.to_string(uuid), endpoint)

    self:require_peer(uuid, endpoint)
  else
    peer = self:find_peer(uuid)
    if peer then self:remove_peer(peer):disconnect() end
  end
end

local function wrap_msg(a, b, ...)
  return a, b, {...}
end

local function Node_on_inbox(self, inbox)
  local log = self:logger()

  local routing_id, msg, content = wrap_msg(inbox:recvx())
  if not routing_id                              then return end
  if #routing_id ~= ZRE_UUID_LEN + 1             then return end
  if not msg                                     then return end

  local uuid     = routing_id:sub(2)

  local iter = Iter(msg)
  if iter:next_bytes(#ZRE_SIGNATURE) ~= ZRE_SIGNATURE           then return end
  local cmd      = iter:next_uint8()  if not cmd                then return end
  local version  = iter:next_uint8()  if version ~= ZRE_VERSION then return end
  local sequence = iter:next_uint16() if not sequence           then return end

  local name = ZRE_COMMANDS_NAME[cmd]
  if not name then
    log.alert("Unknown command ", cmd, " from ", UUID.to_string(uuid))
    return
  end

  log.notice("INBOX : ", UUID.to_string(uuid), name, "#", sequence)

  local fn = MessageDecoder[name]
  if fn then fn(self, version, uuid, sequence, iter, content) end
end

local function Node_on_interval(self)
  local log = self:logger()

  for id, peer in pairs(self._private.peers) do
    if peer:expire() then
      log.info(peer:name(), " ", peer:endpoint(), " - expire")
      self:remove_peer(peer):disconnect()
    end
  end

  for id, peer in pairs(self._private.peers) do
    if peer:evasive() then
      local msg = MessageEncoder.PING(node)
      self:remove_peer(peer):disconnect()
    end
  end

end

local Node_api_dispatch do

local Node_api = {}

function Node_api_dispatch(self, pipe, cmd, ...)
  if not cmd then return nil, ... end
  local fn = Node_api[cmd]
  if fn then return fn(self, pipe, ...) end
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

Node_api[ "SET VERBOSE"  ] = function (self, pipe)
  self:set_log_level("trace")
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
  return pipe:send(self:uuid(true))
end

Node_api[ "NAME"         ] = function (self, pipe)
  return pipe:send(self:name())
end

Node_api[ "BIND"         ] = function (self, pipe, endpoint)
  local ok, err = self:bind(endpoint)
  return pipe:send( ok and "1" or "0")
end

Node_api[ "CONNECT"      ] = function (self, pipe, endpoint)
  local ok, err = self:connect(endpoint)
  return pipe:send( ok and "1" or "0")
end

Node_api[ "START"        ] = function (self, pipe)
  local ok, err = self:start()
  return pipe:send( ok and "1" or "0")
end

Node_api[ "STOP"         ] = function (self, pipe)
  local ok, err = self:stop()
  
  return pipe:send( ok and "1" or "0")
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

local function Node_on_command(self, pipe)
  return Node_api_dispatch(self, pipe, pipe:recvx())
end

function Node:new(pipe, outbox)
  local ctx = zmq.assert(zthreads.context())

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
    port     = ZRE_DISCOVERY_PORT; -- beacon port
    logger   = LogLib.new('none',
      require "log.writer.stdout".new(),
      o:_formatter(require "log.formatter.concat".new())
    )
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

  assert(not p.inbox)

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

    local buf = Buffer()
      :write_bytes(ZRE_PREFIX)
      :write_bytes(self:uuid())
      :write_uint16(port)

    local ok
    if self._private.interval and self._private.interval > 0 then
      ok, err = beacon:interval(self._private.interval)
      if not ok then local_cleanup() return nil, err end
    end

    ok, err = beacon:noecho()            if not ok then local_cleanup() return nil, err end
    ok, err = beacon:publish(buf:data()) if not ok then local_cleanup() return nil, err end
    ok, err = beacon:subscribe("ZRE")    if not ok then local_cleanup() return nil, err end

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
    p.beacon:publish(Buffer()
      :write_bytes(ZRE_PREFIX)
      :write_bytes(self:uuid())
      :write_uint16(0)
    :data())
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

function Node:interrupt()
  return self._private.loop:interrupt()
end

function Node:destroy()
  local p = self._private
  self:stop()
  if p.loop then
    p.loop:destroy()
    p.loop = nil
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
  local group = p.peer_groups[name] or Group:new(name)
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

    peer, err = Peer:new(uuid)
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

return Node