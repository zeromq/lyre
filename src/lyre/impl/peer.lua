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
local ztimer   = require "lzmq.timer"
local zthreads = require "lzmq.threads"
local UUID     = require "lyre.impl.uuid"
local utils    = require "lyre.impl.utils"
local ZRE      = require "lyre.zre"
local bit      = utils.bit

---------------------------------------------------------------------
local Peer = {} do
Peer.__index = Peer

function Peer:new(node, uuid)
  local o = setmetatable({}, self)
  uuid = UUID.new(uuid)
  o._private = {
    node          = node;
    uuid          = uuid;
    name          = uuid:str():sub(1, 6);
    version       = ZRE.VERSION;
    sent_sequence = 0;
    want_sequence = 1;
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
  return self._private.expired_at:rest() == 0
end

function Peer:send(msg)
  local ok, err = msg:send(self, self._private.mailbox)
  if not ok then
    if err:name() == 'EAGAIN' then
      self._private.node:remove_peer(self):disconnect()
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

function Peer:status()
  return self._private.status
end

function Peer:set_status(v)
  self._private.status = v
  return self
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
    p.mailbox, p.endpoint, p.ready = nil
  end
end

end
---------------------------------------------------------------------

return {
  new = function(...) return Peer:new(...) end;
}