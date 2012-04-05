# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "qless/version"

Gem::Specification.new do |s|
  s.name        = "qless"
  s.version     = Qless::VERSION
  s.authors     = ["Dan Lecocq"]
  s.email       = ["dan@seomoz.org"]
  s.homepage    = "http://github.com/seomoz/qless"
  s.summary     = %q{A Redis-Based Queueing System}
  s.description = %q{
    `qless` is meant to be a performant alternative to other queueing
    systems, with statistics collection, a browser interface, and 
    strong guarantees about job losses.
    
    It's written as a collection of Lua scipts that are loaded into the
    Redis instance to be used, and then executed by the client library.
    As such, it's intended to be extremely easy to port to other languages,
    without sacrificing performance and not requiring a lot of logic
    replication between clients. Keep the Lua scripts updated, and your
    language-specific extension will also remain up to date.
  }

  s.rubyforge_project = "qless"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_development_dependency "sinatra", "~> 1.3.2"
  s.add_development_dependency "rspec"  , "~> 2.6"
  s.add_development_dependency "redis"  , "~> 2.2.2"
  s.add_development_dependency "haml"   , "~> 3.1.4"
  s.add_development_dependency "json"   , "~> 1.6.5"
  s.add_development_dependency "uuid"   , "~> 2.3.5"
end