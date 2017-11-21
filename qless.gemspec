# -*- encoding: utf-8 -*-
$LOAD_PATH.push File.expand_path('../lib', __FILE__)
require 'qless/version'

Gem::Specification.new do |s|
  s.name        = 'qless'
  s.version     = Qless::VERSION
  s.authors     = ['Dan Lecocq', 'Myron Marston']
  s.email       = ['dan@moz.com', 'myron@moz.com']
  s.homepage    = 'http://github.com/seomoz/qless'
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

  s.rubyforge_project = 'qless'

  s.files         = %w(README.md Gemfile Rakefile HISTORY.md)
  s.files        += Dir.glob('lib/**/*.rb')
  s.files        += Dir.glob('lib/qless/lua/*.lua')
  s.files        += Dir.glob('exe/**/*')
  s.files        += Dir.glob('lib/qless/server/**/*')
  s.bindir        = 'exe'
  s.executables   = ['qless-web', 'qless-config', 'qless-stats']

  s.test_files    = s.files.grep(/^(test|spec|features)\//)
  s.require_paths = ['lib']

  s.add_dependency 'metriks', '~> 0.9'
  s.add_dependency 'redis', ['>= 2.2', '< 4.0.0.rc1']
  s.add_dependency 'rusage', '~> 0.2.0'
  s.add_dependency 'sentry-raven', '~> 0.15.6'
  s.add_dependency 'sinatra', ['>= 1.3', '< 2.1']
  s.add_dependency 'statsd-ruby', '~> 1.3'
  s.add_dependency 'thin', '~> 1.6'
  s.add_dependency 'thor', '~> 0.19.1'
  s.add_dependency 'vegas', '~> 0.1.11'

end
