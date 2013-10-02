#!/usr/bin/env rake
require 'bundler/gem_helper'
Bundler::GemHelper.install_tasks

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[--profile --format progress]
  t.ruby_opts  = "-Ispec -rsimplecov_setup"
end

# TODO: bump this up as test coverage increases. It was 90.29 when I last updated it on 2012-05-21.
# On travis where we skip JS tests, it's at 90.0 on 2013-10-01
min_coverage_threshold = 85.0
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
  qless_core_dir = "./lib/qless/qless-core"

  desc "Builds the qless-core lua scripts"
  task :build do
    Dir.chdir(qless_core_dir) do
      sh "make clean && make"
      sh "cp qless.lua ../lua"
      sh "cp qless-lib.lua ../lua"
    end
  end

  task :update_submodule do
    Dir.chdir(qless_core_dir) do
      sh "git checkout master"
      sh "git pull --rebase"
    end
  end

  desc "Updates qless-core and rebuilds it"
  task update: [:update_submodule, :build]

  namespace :verify do
    script_files = %w[ lib/qless/lua/qless.lua lib/qless/lua/qless-lib.lua ]

    desc "Verifies the script has no uncommitted changes"
    task :clean do
      script_files.each do |file|
        git_status = `git status -- #{file}`
        unless /working directory clean/.match(git_status)
          raise "#{file} is dirty: \n\n#{git_status}\n\n"
        end
      end
    end

    desc "Verifies the script is current"
    task :current do
      require 'digest/md5'
      our_md5s = script_files.map do |file|
        Digest::MD5.hexdigest(File.read file)
      end

      canonical_md5s = Dir.chdir(qless_core_dir) do
        sh "make clean && make"
        script_files.map do |file|
          Digest::MD5.hexdigest(File.read(File.basename file))
        end
      end

      unless our_md5s == canonical_md5s
        raise "The current scripts are out of date with qless-core"
      end
    end
  end

  desc "Verifies the committed script is current"
  task verify: %w[ verify:clean verify:current ]
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

