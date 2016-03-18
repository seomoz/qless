# Encoding: utf-8

# The thing we're testing
require 'qless/worker'

# Standard
require 'logger'

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
      let(:worker) do
        worker_class.new(
          reserver,
          output: log_output,
          log_level: Logger::DEBUG)
      end
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
        first_line_in_perform_method_regex = /base\.rb:\d+:in `perform'/
        job.should_receive(:fail) do |group, message|
          last_line = message.split("\n").last
          expect(last_line).to match(first_line_in_perform_method_regex)
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
        JobClass.stub(:perform) { |j| j.requeue('other') }
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

      it 'uses log specified in options' do
        logger_io = StringIO.new
        logger = Logger.new(logger_io)
        worker = worker_class.new(reserver, logger: logger, log_level: Logger::DEBUG)

        JobClass.stub(:perform)
        worker.send(:log, :warn, 'my-message')
        expect(logger_io.string).to match(/my-message/)
      end

      it 'reports log_level when configures log in worker' do
        worker = worker_class.new(reserver, output: log_output, log_level: Logger::ERROR)

        expect(worker.log_level).to eq(Logger::ERROR)
      end

      it 'defaults log_level to warn when configures log in worker with default' do
        worker = worker_class.new(reserver, output: log_output)

        expect(worker.log_level).to eq(Logger::WARN)
      end

      it 'reports log_level when logger passed in options' do
        logger = Logger.new(StringIO.new)
        logger.level = Logger::DEBUG
        worker = worker_class.new(reserver, logger: logger)

        expect(worker.log_level).to eq(Logger::DEBUG)
      end

    end

    describe Workers::SerialWorker do
      let(:worker_class) { Workers::SerialWorker }

      include_context 'with a dummy client'
      it_behaves_like 'a worker'
    end

    describe Workers::ForkingWorker do
      let(:worker_class) { Workers::ForkingWorker }

      include_context 'with a dummy client'
      it_behaves_like 'a worker'
    end
  end
end
