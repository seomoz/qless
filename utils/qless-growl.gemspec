# -*- encoding: utf-8 -*-
$LOAD_PATH.push File.expand_path('../../lib', __FILE__)
require 'qless/version'

Gem::Specification.new do |s|
  s.name        = 'qless-growl'
  s.version     = Qless::VERSION
  s.authors     = ['Dan Lecocq']
  s.email       = ['dan@seomoz.org']
  s.homepage    = 'http://github.com/seomoz/qless'
  s.summary     = %q{Growl Notifications for Qless}
  s.description = %q{
    Get Growl notifications for jobs you're tracking in your qless
    queue.
  }

  s.rubyforge_project = 'qless-growl'

  s.files         = Dir.glob('../bin/qless-growl')
  s.executables   = ['qless-growl']

  s.add_dependency 'qless'         , '~> 0.9'
  s.add_dependency 'ruby-growl'    , '~> 4.0'
  s.add_dependency 'micro-optparse', '~> 1.1'
end
