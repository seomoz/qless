#!/usr/bin/env rake

require 'bundler'
Bundler.setup
require "bundler/gem_tasks"

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[--profile --format progress]
  t.ruby_opts  = "-Ispec -rsimplecov_setup"
end

# TODO: bump this up as test coverage increases. It was 90.29 when I last updated it on 2012-05-21.
# On travis where we skip JS tests, it's at 83.9 on 2013-01-15
min_coverage_threshold = 83.5
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

namespace :core do
  desc "Builds the qless-core lua script"
  task :build do
    Dir.chdir("./lib/qless/qless-core") do
      sh "make clean && make"
      sh "cp qless.lua .."
    end
  end
end

require 'qless/tasks'

namespace :qless do
  task :setup do
    require 'qless'
    queue = Qless::Client.new.queues["example"]
    queue.client.redis.flushdb

    ENV['QUEUES'] = queue.name
    ENV['VVERBOSE'] = '1'

    class ExampleJob
      def self.perform(job)
        sleep_time = job.data.fetch("sleep")
        print "Sleeping for #{sleep_time}..."
        sleep sleep_time
        puts "done"
      end
    end

    20.times do |i|
      queue.put(ExampleJob, sleep: i)
    end
  end
end

