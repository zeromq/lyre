local zloop = require "lzmq.loop"
local Lyre  = require "Lyre"

local name = arg[1]
if not name then
  print("usage:"
  print("  lyrechat name")
end

local node = Lyre.Node()

assert(node
  :set_name(name)
  :join("CHAT")
  -- :set_verbose()
  :start()
)

loop = zloop.new()

loop:add_socket(node, function(s)
  node:recv()
end)

loop:add_interval(5000, function()
  node:shout("CHAT", "Hello from Lyre")
end)

loop:start()
