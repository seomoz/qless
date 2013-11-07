require 'spec_helper'
require 'support/forking_worker_context'
require 'qless/middleware/memory_usage_monitor'

module Qless
  module Middleware
    describe MemoryUsageMonitor do
      include_context "forking worker"

      mem_usage_from_other_technique = nil

      shared_examples_for "memory usage monitor" do
        it 'can report the amount of memory the process is using' do
          mem = MemoryUsageMonitor.current_usage

          # We expect mem usage to be at least 10MB, but less than 10GB
          expect(mem).to be > 10_000_000
          expect(mem).to be < 10_000_000_000

          if mem_usage_from_other_technique
            expect(mem).to be_within(10).percent_of(mem_usage_from_other_technique)
          else
            mem_usage_from_other_technique = mem
          end
        end

        it 'does not allow a bloated job to cause a child to permanently retain the memory blot' do
          bloated_job_class = Class.new do
            def self.perform(job)
              job_record = JobRecord.new(Process.pid, Qless::Middleware::MemoryUsageMonitor.current_usage, nil)
              job_record.after_mem = bloat_memory(job_record.before_mem, job.data.fetch("target"))

              # publish what the memory usage was before/after
              job.client.redis.rpush('mem_usage', Marshal.dump(job_record))
            end

            def self.bloat_memory(original_mem, target)
              current_mem = original_mem

              while current_mem < target
                SecureRandom.hex(
                  # The * 10 is based on experimentation, taking into account
                  # the fact that target/current are in bytes
                  (target - current_mem) / 10
                ).to_sym # symbols are never GC'd.

                print '.'
                current_mem = Qless::Middleware::MemoryUsageMonitor.current_usage
              end

              puts "Final: #{current_mem}"
              current_mem
            end

            def self.print(msg)
              super if ENV['DEBUG']
            end

            def self.puts(msg)
              super if ENV['DEBUG']
            end
          end

          stub_const("JobRecord", Struct.new(:pid, :before_mem, :after_mem))

          stub_const('BloatedJobClass', bloated_job_class)

          [25, 60, 25].each do |target_mb|
            queue.put(BloatedJobClass, { target: target_mb * (1_000_000) })
          end

          job_records = []

          worker.extend(Qless::Middleware::MemoryUsageMonitor.new(
            max_memory: 50_000_000
          ))

          run_worker_concurrently_with(worker) do
            3.times do
              _, result = client.redis.brpop('mem_usage', timeout: 20)
              job_records << Marshal.load(result)
            end
          end

          # the second job should increase mem growth but be the same pid.
          expect(job_records[1].pid).to eq(job_records[0].pid)
          expect(job_records[1].before_mem).to be > job_records[0].before_mem

          # the third job sould be a new process with cleared out memory
          expect(job_records[2].pid).not_to eq(job_records[0].pid)
          expect(job_records[2].before_mem).to be < job_records[1].before_mem
        end
      end

      context "when the proc-wait3 gem is available" do
        before do
          load "qless/middleware/memory_usage_monitor.rb"

          unless Process.respond_to?(:getrusage)
            pending "Could not load the proc-wait3 gem"
          end
        end

        include_examples "memory usage monitor"
      end

      context "when the proc-wait3 gem is not available" do
        before do
          MemoryUsageMonitor.stub(:warn)
          MemoryUsageMonitor.stub(:require).and_raise(LoadError)
          load "qless/middleware/memory_usage_monitor.rb"
        end

        include_examples "memory usage monitor"
      end
    end
  end
end

