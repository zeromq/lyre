--
-- Start Lyre Node join to group CHAT and 
-- shout message every 5 second
--

local zloop = require "lzmq.loop"
local Lyre  = require "lyre"


local interface, name = arg[1], arg[2]
if not name then name, interface = interface end

if not name then
  print("usage:")
  print("  lyrechat [interface] name")
  os.exit(-1)
end

local node = Lyre.Node(name)
  :set_log_writer([[
    return require"log.writer.console.color".new()
  ]])
  :set_verbose("info")
  :join("CHAT")

if interface then
  node:set_host(interface)
end

node:start()

print("****************************************************")
print(" My name    : " .. node:name()                       )
print(" My uuid    : " .. node:uuid()                       )
print(" My endpoint: " .. node:endpoint()                   )
print("****************************************************")

loop = zloop.new()

loop:add_socket(node, function(s)
  print(node:recv())
end)

loop:add_interval(5000, function()
  node:shout("CHAT", "Hello from Lyre")
end)

loop:start()
