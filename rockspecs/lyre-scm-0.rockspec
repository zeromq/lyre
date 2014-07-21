package = "lyre"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lyre/archive/master.zip",
  dir = "lyre-master",
}

description = {
  summary  = "Lua port of Zyre",
  homepage = "https://github.com/moteus/lyre",
  license  = "MIT/X11",
}

dependencies = {
  "lua >= 5.1, < 5.3",
  "luuid",
  "struct >= 1.4 ",
  "bit32",
  "lua-log",
  "luasocket",
  "lua-llthreads2",
  "lzmq-beacon",
  "lzmq",
}

build = {
  copy_directories = {"examples"},

  type = "builtin",

  modules = {
    ["lyre"              ] = "src/lyre.lua";
    ["lyre.zre"          ] = "src/lyre/zre.lua";
    ["lyre.impl.peer"    ] = "src/lyre/impl/peer.lua";
    ["lyre.impl.uuid"    ] = "src/lyre/impl/uuid.lua";
    ["lyre.impl.node"    ] = "src/lyre/impl/node.lua";
    ["lyre.impl.utils"   ] = "src/lyre/impl/utils.lua";
    ["lyre.impl.group"   ] = "src/lyre/impl/group.lua";
    ["lyre.impl.message" ] = "src/lyre/impl/message.lua";
  },
}











