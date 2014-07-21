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

return {
  new = function(...) return Group:new(...) end;
}
