---------------------------------------------------------------------
local function lyre_node_thread(pipe, outbox)
  local zmq  = require "lzmq"
  local zth  = require "lzmq.threads"
  local Node = require "lyre.impl.node"

  outbox = zmq.assert(zth.context():socket{zmq.PAIR, connect = outbox})
  local node = Node:new(pipe, outbox)

  node:run()
  node:destroy()
  outbox:destroy()
end
---------------------------------------------------------------------

local zmq = require "lzmq"
local zth = require "lzmq.threads"

local function rand_str(n)
  local t = {}
  for i = 1, n do t[i] = string.char( math.random(string.byte('a'), string.byte('z')) ) end
  return table.concat(t)
end

local function make_pipe(ctx)
  local pipe = ctx:socket(zmq.PAIR)
  local pipe_endpoint = "inproc://lyre.outbox." .. pipe:fd() .. "." .. rand_str(10);
  local ok, err = pipe:bind(pipe_endpoint)
  if not ok then 
    pipe:close()
    return nil, err
  end
  return pipe, pipe_endpoint
end

---------------------------------------------------------------------
local Node = {} do
Node.__index = Node

function Node:new(ctx)
  if not ctx then ctx = zth.context() end

  local inbox, endpoind = make_pipe(ctx)
  if not inbox then return nil, endpoind end
  
  local actor = zth.actor(ctx, lyre_node_thread, endpoind)

  local ok, err = actor:start()
  if not ok then 
    inbox:close()
    return nil, err
  end

  local o = setmetatable({},self)
  
  o._private = {
    actor = actor;
    inbox = inbox;
  }
  return o
end

function Node:_send(...)
  return self._private.actor:sendx(...)
end

function Node:_recv()
  return self._private.actor:recvx()
end

function Node:socket()
  return self._private.inbox
end

function Node:recv()
  return self._private.inbox:recvx()
end

function Node:uuid()
  if not self._private.uuid then
    self:_send("UUID")
    self._private.uuid = self:_recv()
  end
  return self._private.uuid
end

function Node:name()
  if not self._private.name then
    self:_send("NAME")
    self._private.name = self:_recv()
  end
  return self._private.name
end

function Node:set_name(name)
  self._private.name = nil
  self:_send("SET NAME", name)
  return self
end

function Node:set_header(key, value)
  self:_send("SET HEADER", key, value)
  return self
end

function Node:set_verbose()
  self:_send("SET VERBOSE")
  return self
end

function Node:set_port(port)
  assert(type(port) == 'number')
  self:_send("SET PORT", tostring(port))
  return self
end

function Node:set_host(host)
  self:_send("SET HOST", host)
  return self
end

function Node:set_interval(interval)
  assert(type(interval) == 'number')
  self:_send("SET INTERVAL", tostring(interval))
  return self
end

function Node:start()
  self:_send("START")
  local ok, err = self:_recv()
  if not ok then return nil, err end
  return ok == "1"
end

function Node:stop()
  self:_send("STOP")
  local ok, err = self:_recv()
  if not ok then return nil, err end
  return ok == "1"
end

function Node:join(group)
  self:_send("JOIN", group)
  return self
end

function Node:leave(group)
  self:_send("LEAVE", group)
  return self
end

function Node:whisper(peer, ...)
  self:_send("WHISPER", peer, ...)
  return self
end

function Node:shout(group, ...)
  self:_send("SHOUT", group, ...)
  return self
end

function Node:destroy()
  local actor = self._private.actor
  if actor then
    actor:sendx("$TERM")
    actor:join()
    self._private.actor = nil
  end
end

end
---------------------------------------------------------------------

local Lyre = {}

function Lyre.Node(...)
  return Node:new(...)
end

return Lyre
