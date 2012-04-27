#!/usr/bin/env rake

require 'bundler'
Bundler.setup
require "bundler/gem_tasks"

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[--profile --format progress]
  t.ruby_opts  = "-Ispec -rsimplecov_setup"
end

# TODO: bump this up as test coverage increases. It was 77.16 when I added simplecov on 2012-04-27.
min_coverage_threshold = 77.0
desc "Checks the spec coverage and fails if it is less than #{min_coverage_threshold}%"
task :check_coverage do
  percent = File.read("./coverage/coverage_percent.txt").to_f
  if percent < min_coverage_threshold
    raise "Spec coverage was not high enough: #{percent.round(2)}%"
  else
    puts "Nice job!  Spec coverage is still at least #{min_coverage_threshold}%"
  end
end

task default: [:spec, :check_coverage]


require 'qless/tasks'
