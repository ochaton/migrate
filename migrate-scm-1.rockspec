package = "migrate"
version = "scm-1"
source = {
   url = "git+https://github.com/orchaton/migrate.git"
}
description = {
   summary = "Parser for Tarantool 1.5 binary files",
   homepage = "https://github.com/orchaton/migrate",
   license = "BSD"
}
dependencies = {
   "lua ~> 5.1",
   "bin scm-4",
   "ffi-reloadable scm-1",
}
build = {
   type = "builtin",
   modules = {
      ["migrate"] = "migrate/init.lua",
      ["migrate.parser"] = "migrate/parser.lua"
   }
}
