# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "cql_driver/version"

Gem::Specification.new do |s|
  s.name        = "cql_driver"
  s.version     = CqlDriver::VERSION
  s.authors     = ["Brent Theisen"]
  s.email       = ["brent@bantamlabs.com"]
  s.summary     = %q{Ruby driver for the Cassandra CQL binary protocol}
  s.description = %q{Ruby driver for the Cassandra CQL binary protocol}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
