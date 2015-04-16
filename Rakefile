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

task default: [:spec, :check_coverage, :cane]

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

desc "Starts a qless console"
task :console do
  ENV['PUBLIC_SEQUEL_API'] = 'true'
  ENV['NO_NEW_RELIC'] = 'true'
  exec "bundle exec pry -r./conf/console"
end

require 'qless/tasks'

namespace :qless do
  desc "Runs a test worker so you can send signals to it for testing"
  task :run_test_worker do
    require 'qless'
    require 'qless/job_reservers/ordered'
    require 'qless/worker'
    queue = Qless::Client.new.queues["example"]
    queue.client.redis.flushdb

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

    reserver = Qless::JobReservers::Ordered.new([queue])
    Qless::Workers::ForkingWorker.new(reserver, log_level: Logger::INFO).run
  end
end


namespace :cane do
  begin
    require 'cane/rake_task'

    libs = [
      { name: 'qless', dir: '.', root: '.' },
    ]

    libs.each do |lib|
      desc "Runs cane code quality checks for #{lib[:name]}"
      Cane::RakeTask.new(lib[:name]) do |cane|
        cane.no_doc   = true

        cane.abc_glob = "#{lib[:dir]}/{lib,spec}/**/*.rb"
        cane.abc_max = 15
        cane.abc_exclude = %w[
          Middleware::(anon)#expect_job_to_timeout
          Qless::Job#initialize
          Qless::Middleware::RequeueExceptions#handle_exception
          Qless::Middleware::Timeout#initialize
          Qless::WorkerHelpers#run_jobs
          Qless::Workers::BaseWorker#initialize
          Qless::Workers::BaseWorker#register_signal_handlers
          Qless::Workers::ForkingWorker#register_signal_handlers
          Qless::Workers::SerialWorker#run
        ]

        cane.style_glob = "#{lib[:dir]}/lib/**/*.rb"
        cane.style_measure = 100
        cane.style_exclude = %w[
        ]
      end
    end

    desc "Runs cane code quality checks for all projects"
    task all: libs.map { |l| l[:name] }

  rescue LoadError
    task :all do
      puts "cane is not supported in ruby #{RUBY_VERSION}"
    end
  end
end

task cane: "cane:all"
