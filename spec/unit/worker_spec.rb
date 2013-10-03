# Encoding: utf-8

# The thing we're testing
require 'qless/worker'

# Spec
require 'spec_helper'

module Qless
  describe Workers do
    shared_context 'with a dummy client' do
      # Our client should ignore everything
      let(:client) { double('client').as_null_object }

      # Our doubled reserver doesn't do much
      let(:reserver) do
        instance_double('Qless::JobReservers::Ordered',
                        description: 'job reserver',
                        queues: [],
                        prep_for_work!: nil)
      end

      # A place to write to
      let(:log_output) { StringIO.new }

      # to account for the fact that we format the backtrace lines...
      let(:__file__) { __FILE__.split(File::SEPARATOR).last }

      # A dummy job class
      class JobClass; end
      # A job of that dummy job class
      let(:job) { Job.build(client, JobClass) }
    end

    shared_examples_for 'a worker' do
      before { clear_qless_memoization }
      after(:all) { clear_qless_memoization }

      it 'performs the job' do
        JobClass.should_receive(:perform)
        worker.perform(Job.build(client, JobClass))
      end

      it 'fails the job it raises an error, including root exceptions' do
        JobClass.stub(:perform) { raise Exception.new('boom') }
        expected_line_number = __LINE__ - 1
        job.should respond_to(:fail).with(2).arguments
        job.should_receive(:fail) do |group, message|
          group.should eq('Qless::JobClass:Exception')
          message.should include('boom')
          message.should include("#{__file__}:#{expected_line_number}")
        end
        worker.perform(job)
      end

      it 'removes the redundant backtrace lines from failure backtraces' do
        JobClass.stub(:perform) { raise Exception.new('boom') }
        job.should respond_to(:fail).with(2).arguments
        job.should_receive(:fail) do |group, message|
          last_line = message.split("\n").last
          expect(last_line).to match(/base\.rb:\d+:in `around_perform'/)
        end
        worker.perform(job)
      end

      it 'replaces the working directory with `.` in failure backtraces' do
        JobClass.stub(:perform) { raise Exception.new('boom') }
        job.should respond_to(:fail).with(2).arguments
        job.should_receive(:fail) do |group, message|
          expect(message).not_to include(Dir.pwd)
          expect(message).to include('./lib')
        end
        worker.perform(job)
      end

      it 'truncates failure messages so they do not get too big' do
        failure = 'a' * 50_000
        JobClass.stub(:perform) { raise Exception.new(failure) }
        job.should respond_to(:fail).with(2).arguments
        job.should_receive(:fail) do |group, message|
          expect(message.bytesize).to be < 25_000
        end
        worker.perform(job)
      end

      it 'replaces the GEM_HOME with <GEM_HOME> in failure backtraces' do
        gem_home = '/this/is/gem/home'
        with_env_vars 'GEM_HOME' => gem_home do
          JobClass.stub(:perform) do
            error = Exception.new('boom')
            error.set_backtrace(["#{gem_home}/foo.rb:1"])
            raise error
          end
          job.should respond_to(:fail).with(2).arguments
          job.should_receive(:fail) do |group, message|
            expect(message).not_to include(gem_home)
            expect(message).to include('<GEM_HOME>/foo.rb:1')
          end
          worker.perform(job)
        end
      end

      it 'completes the job if it finishes with no errors' do
        JobClass.stub(:perform)
        job.should respond_to(:complete).with(0).arguments
        job.should_receive(:complete).with(no_args)
        worker.perform(job)
      end

      it 'fails the job if the job class is invalid or not found' do
        hide_const('Qless::MyJobClass')
        job.should_receive(:fail)
        expect { worker.perform(job) }.not_to raise_error
      end

      it 'does not complete the job its state has changed' do
        JobClass.stub(:perform) { |j| j.move('other') }
        job.should_not_receive(:complete)
        worker.perform(job)
      end

      it 'supports middleware modules' do
        mixin = Module.new do
          define_method :around_perform do |job|
            # Send job the foo method
            job.foo
          end
        end
        worker.extend(mixin)
        job.should_receive(:foo)
        worker.perform(job)
      end

      it 'fails the job if a middleware module raises an error' do
        worker.extend Module.new {
          def around_perform(job)
            raise 'boom'
          end
        }
        expected_line_number = __LINE__ - 3
        job.should respond_to(:fail).with(2).arguments
        job.should_receive(:fail) do |group, message|
          message.should include('boom')
          message.should include("#{__file__}:#{expected_line_number}")
        end
        worker.perform(job)
      end
    end

    describe Workers::SerialWorker do
      let(:worker) do
        Workers::SerialWorker.new(
          reserver,
          output: log_output,
          log_level: Logger::DEBUG)
      end

      include_context 'with a dummy client'
      it_behaves_like 'a worker'
    end

    describe Workers::ForkingWorker do
      let(:worker) do
        Workers::ForkingWorker.new(
          reserver,
          output: log_output,
          log_level: Logger::DEBUG)
      end

      include_context 'with a dummy client'
      it_behaves_like 'a worker'
    end
  end
end

#     shared_examples_for 'a working worker' do
#       describe '#work' do
#         around(:each) do |example|
#           old_procline = procline
#           example.run
#           $0 = old_procline
#         end
#
#         it 'begins with a "starting" procline' do
#           starting_procline = nil
#           reserver.stub(:reserve) do
#             starting_procline = procline
#             nil
#           end

#           worker.work(0)
#           starting_procline.should include('Starting')
#         end
#
#         it 'can be unpaused' do
#           worker.pause
#
#           paused_checks = 0
#           old_paused = worker.method(:paused)
#           worker.stub(:paused) do
#             paused_checks += 1 # count the number of loop iterations
#             worker.unpause if paused_checks == 20 # so we don't loop forever
#             old_paused.call
#           end
#
#           worker.work(0)
#           paused_checks.should be >= 20
#         end
#
#         context 'when an error occurs while reserving a job' do
#           before { reserver.stub(:reserve) { raise 'redis error' } }
#
#           it 'does not kill the worker' do
#             expect { worker.work(0) }.not_to raise_error
#           end
#
#           it 'logs the error' do
#             worker.work(0)
#             expect(log_output.string).to include('redis error')
#           end
#         end
#       end
#     end
#   end
# end
