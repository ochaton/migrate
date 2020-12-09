package = "migrate"
version = "scm-1"
source = {
   url = "https://github.com/orchaton/migrate"
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
      ["migrate.pairs"] = "migrate/pairs.lua",
      ["migrate.parser"] = "migrate/parser.lua"
   }
}
