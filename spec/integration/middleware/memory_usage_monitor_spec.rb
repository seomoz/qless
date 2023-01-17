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
          mem = MemoryUsageMonitor.current_usage_in_kb

          # We expect mem usage to be at least 10MB, but less than 10GB
          expect(mem).to be > 10_000
          expect(mem).to be < 10_000_000

          if mem_usage_from_other_technique
            expect(mem).to be_within(10).percent_of(mem_usage_from_other_technique)
          else
            mem_usage_from_other_technique = mem
          end
        end

        it 'does not allow a bloated job to cause a child to permanently retain the memory bloat' do
          max_memory = (MemoryUsageMonitor.current_usage_in_kb * 1.5).to_i

          [(max_memory / 2), (max_memory * 1.1).to_i, (max_memory / 2)].each do |target|
            queue.put(bloated_job_class, { target: target })
          end

          job_records = []

          worker.extend(MemoryUsageMonitor.new(max_memory: max_memory))

          run_worker_concurrently_with(worker) do
            3.times do
              _, result = client.redis.brpop('mem_usage', timeout: ENV['TRAVIS'] ? 60 : 20)
              job_records << Marshal.load(result)
            end
          end

          # the second job should increase mem growth but be the same pid.
          expect(job_records[1].pid).to eq(job_records[0].pid)
          expect(job_records[1].before_mem).to be > job_records[0].before_mem

          # the third job sould be a new process with cleared out memory
          expect(job_records[2].pid).not_to eq(job_records[0].pid)
          expect(job_records[2].before_mem).to be < job_records[1].before_mem

          expect(log_io.string).to match(/Exiting after job 2/)
        end

        it 'checks the memory usage even if there was an error in the job' do
          failing_job_class = Class.new do
            def self.perform(job)
              job.client.redis.rpush('pid', Process.pid)
              raise "boom"
            end
          end

          stub_const('FailingJobClass', failing_job_class)
          2.times { queue.put(FailingJobClass, {}) }

          worker.extend(MemoryUsageMonitor.new(max_memory: 1)) # force it to exit after every job

          pids = []

          run_worker_concurrently_with(worker) do
            2.times do
              _, result = client.redis.brpop('pid', timeout: ENV['TRAVIS'] ? 60 : 20)
              pids << result
            end
          end

          expect(pids[1]).not_to eq(pids[0])
        end

        let(:bloated_job_class) do
          bloated_job_class = Class.new do
            def self.perform(job)
              job_record = JobRecord.new(Process.pid, Qless::Middleware::MemoryUsageMonitor.current_usage_in_kb, nil)
              job_record.after_mem = bloat_memory(job_record.before_mem, job.data.fetch("target"))

              # publish what the memory usage was before/after
              job.client.redis.rpush('mem_usage', Marshal.dump(job_record))
            end

            def self.bloat_memory(original_mem, target)
              current_mem = original_mem

              while current_mem < target
                SecureRandom.hex(
                  # The * 100 is based on experimentation, taking into account
                  # the fact that target/current are in KB
                  (target - current_mem) * 100
                ).to_sym # symbols are never GC'd.

                print '.'
                current_mem = Qless::Middleware::MemoryUsageMonitor.current_usage_in_kb
              end

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
        end
      end

      context "when the rusage gem is available" do
        it_behaves_like "memory usage monitor" do
          before do
            load "qless/middleware/memory_usage_monitor.rb"

            unless Process.respond_to?(:rusage)
              pending "Could not load the rusage gem"
            end
          end
        end

        let(:memory_kb_according_to_ps) { MemoryUsageMonitor::SHELL_OUT_FOR_MEMORY.() }

        context "when rusage returns memory in KB (commonly on Linux)" do
          before do
            Process.stub(:rusage) { double(maxrss: memory_kb_according_to_ps) }
            load "qless/middleware/memory_usage_monitor.rb"
          end

          it 'returns the memory in KB' do
            expect(MemoryUsageMonitor.current_usage_in_kb).to be_within(1).percent_of(memory_kb_according_to_ps)
          end
        end

        context "when rusage returns memory in bytes (commonly on OS X)" do
          before do
            Process.stub(:rusage) { double(maxrss: memory_kb_according_to_ps * 1024) }
            load "qless/middleware/memory_usage_monitor.rb"
          end

          it 'returns the memory in KB' do
            expect(MemoryUsageMonitor.current_usage_in_kb).to be_within(1).percent_of(memory_kb_according_to_ps)
          end
        end
      end

      context "when the rusage gem is not available" do
        it_behaves_like "memory usage monitor" do
          before do
            MemoryUsageMonitor.stub(:warn)
            MemoryUsageMonitor.stub(:require).and_raise(LoadError)
            load "qless/middleware/memory_usage_monitor.rb"
          end
        end
      end
    end
  end
end

