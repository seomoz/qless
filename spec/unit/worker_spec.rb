# Encoding: utf-8

require 'spec_helper'
require 'yaml'
require 'qless/worker'

module Qless
  describe Worker do
    # Our doubled reserver
    let(:reserver) do
      fire_double('Qless::JobReservers::Ordered',
                  description: 'job reserver', queues: [], prep_for_work!: nil)
    end

    # Our client should ignore everything
    let(:client) { stub.as_null_object }

    before { Subscriber.stub(:start) }

    def procline
      $PROGRAM_NAME
    end

    # Job that writes the program name to the file, and optionally sleeps
    class FileWriterJob
      def self.perform(job)
        sleep(job['sleep']) if job['sleep']
        File.open(job['file'], 'w') { |f| f.write("done: #{$PROGRAM_NAME}") }
      end
    end

    let(:job_output_file) { File.join(temp_dir, "job.out.#{Time.now.to_i}") }
    let(:log_output) { StringIO.new }
    let(:job) do
      Job.build(client, FileWriterJob,
                data: { 'file' => job_output_file })
    end

    let(:temp_dir) { './spec/tmp' }
    before do
      FileUtils.rm_rf temp_dir
      FileUtils.mkdir_p temp_dir
      reserver.stub(:reserve).and_return(job, nil)
    end

    # Return a new middleware module
    def middleware_module(num)
      Module.new do
        define_method :around_perform do |job|
          # Write the before file
          File.open("#{job['file']}.before#{num}", 'w') do |f|
            f.write("before#{num}")
          end
          super(job)
          # Write the after file
          File.open("#{job['file']}.after#{num}", 'w') do |f|
            f.write("after#{num}")
          end
        end
      end
    end

    # to account for the fact that we format the backtrace lines...
    let(:__file__) { __FILE__.split(File::SEPARATOR).last }

    shared_examples_for 'a working worker' do
      describe '#perform' do
        before { clear_qless_memoization }
        after(:all) { clear_qless_memoization }

        class MyJobClass; end
        let(:job) { Job.build(client, MyJobClass) }

        it 'performs the job' do
          MyJobClass.should_receive(:perform)
          worker.perform(job)
        end

        it 'fails the job it raises an error, including root exceptions' do
          MyJobClass.stub(:perform) { raise Exception.new('boom') }
          expected_line_number = __LINE__ - 1
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            group.should eq('Qless::MyJobClass:Exception')
            message.should include('boom')
            message.should include("#{__file__}:#{expected_line_number}")
          end

          worker.perform(job)
        end

        it 'removes the redundant backtrace lines from failure backtraces' do
          MyJobClass.stub(:perform) { raise Exception.new('boom') }
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            last_line = message.split("\n").last(2).first
            expect(last_line).to match(/worker\.rb:\d+:in `around_perform'/)
          end

          worker.perform(job)
        end

        it 'replaces the working directory with `.` in failure backtraces' do
          MyJobClass.stub(:perform) { raise Exception.new('boom') }
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            expect(message).not_to include(Dir.pwd)
            expect(message).to include('./lib')
          end

          worker.perform(job)
        end

        it 'truncates failure messages so they do not get too big' do
          failure_message = 'a' * 50_000
          MyJobClass.stub(:perform) { raise Exception.new(failure_message) }
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            expect(message.bytesize).to be < 25_000
          end

          worker.perform(job)
        end

        it 'replaces the GEM_HOME with <GEM_HOME> in failure backtraces' do
          gem_home = '/this/is/gem/home'
          with_env_vars 'GEM_HOME' => gem_home do
            MyJobClass.stub(:perform) do
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
          MyJobClass.stub(:perform)
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
          MyJobClass.stub(:perform) { |j| j.move('other') }
          job.should_not_receive(:complete)
          worker.perform(job)
        end
      end

      describe '#work' do
        around(:each) do |example|
          old_procline = procline
          example.run
          $0 = old_procline
        end

        it 'performs the job in a process and it completes' do
          worker.work(0)
          File.read(job_output_file).should include('done')
        end

        it 'supports middleware modules' do
          worker.extend middleware_module(1)
          worker.extend middleware_module(2)

          worker.work(0)
          File.read(job_output_file + '.before1').should eq('before1')
          File.read(job_output_file + '.after1').should eq('after1')
          File.read(job_output_file + '.before2').should eq('before2')
          File.read(job_output_file + '.after2').should eq('after2')
        end

        it 'fails the job if a middleware module raises an error' do
          expected_line_number = __LINE__ + 3
          worker.extend Module.new {
            def around_perform(job)
              raise 'boom'
            end
          }

          job.should respond_to(:fail).with(2).arguments
          job.should_receive(:fail) do |group, message|
            message.should include('boom')
            message.should include("#{__file__}:#{expected_line_number}")
          end

          worker.perform(job)
        end

        it 'begins with a "starting" procline' do
          starting_procline = nil
          reserver.stub(:reserve) do
            starting_procline = procline
            nil
          end

          worker.work(0)
          starting_procline.should include('Starting')
        end

        it 'can be unpaused' do
          worker.pause

          paused_checks = 0
          old_paused = worker.method(:paused)
          worker.stub(:paused) do
            paused_checks += 1 # count the number of loop iterations
            worker.unpause if paused_checks == 20 # so we don't loop forever
            old_paused.call
          end

          worker.work(0)
          paused_checks.should be >= 20
        end

        context 'when an error occurs while reserving a job' do
          before { reserver.stub(:reserve) { raise 'redis error' } }

          it 'does not kill the worker' do
            expect { worker.work(0) }.not_to raise_error
          end

          it 'logs the error' do
            worker.work(0)
            expect(log_output.string).to include('redis error')
          end
        end
      end
    end

    context 'multi process' do
      let(:worker) { Worker.new(reserver, output: log_output, verbose: true) }
      it_behaves_like 'a working worker'
      after { worker.send :stop! }

      it 'stops working when told to shutdown' do
        pending('This should be worked on')
      end

      it 'can be paused' do
        pending('This should be worker on')
      end
    end

    describe '.start' do
      def with_env_vars(vars = {})
        defaults = {
          'REDIS_URL' => redis_url,
          'QUEUE' => 'mock_queue'
        }
        super(defaults.merge(vars)) { yield }
      end

      it 'starts working with sleep interval INTERVAL' do
        with_env_vars 'INTERVAL' => '2.3' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options|
            worker = orig_new.call(reserver, options)
            worker.interval.should eq(2.3)
            stub.as_null_object
          end
          Worker.start
        end
      end

      it 'defaults the sleep interval to 5.0' do
        with_env_vars do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver)
            worker.interval.should eq(5.0)
            stub.as_null_object
          end
          Worker.start
        end
      end

      it 'starts working with sleep max_startup_interval MAX_STARTUP_INTERVAL' do
        with_env_vars 'MAX_STARTUP_INTERVAL' => '2.3' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options|
            worker = orig_new.call(reserver, options)
            worker.max_startup_interval.should eq(2.3)
            stub.as_null_object
          end
          Worker.start
        end
      end

      it 'defaults the sleep max_startup_interval to 10.0' do
        with_env_vars do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver)
            worker.max_startup_interval.should eq(10.0)
            stub.as_null_object
          end
          Worker.start
        end
      end

      it 'uses the named QUEUE' do
        with_env_vars 'QUEUE' => 'normal' do
          Worker.should_receive(:new) do |reserver|
            reserver.queues.map(&:name).should eq(['normal'])
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'uses the named QUEUES (comma delimited)' do
        with_env_vars 'QUEUES' => 'high,normal, low' do
          Worker.should_receive(:new) do |reserver|
            reserver.queues.map(&:name).should eq(%w{high normal low})
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'raises an error if no queues are provided' do
        with_env_vars 'QUEUE' => '', 'QUEUES' => '' do
          expect do
            Worker.start
          end.to raise_error(/must pass QUEUE or QUEUES/)
        end
      end

      it 'uses the Ordered reserver by default' do
        with_env_vars do
          Worker.should_receive(:new) do |reserver|
            reserver.should be_a(JobReservers::Ordered)
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'uses the RoundRobin reserver if so configured' do
        with_env_vars 'JOB_RESERVER' => 'RoundRobin' do
          Worker.should_receive(:new) do |reserver|
            reserver.should be_a(JobReservers::RoundRobin)
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets logging level correctly' do
        with_env_vars do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(
              reserver, options.merge(output: StringIO.new))
            worker.log_level.should be(Logger::WARN)
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets logging level appropriately when passed VERBOSE' do
        with_env_vars 'VERBOSE' => '1' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(
              reserver, options.merge(output: StringIO.new))
            worker.log_level.should be(Logger::INFO)
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets logging level appropriately when passed VVERBOSE' do
        with_env_vars 'VVERBOSE' => '1' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(
              reserver, options.merge(output: StringIO.new))
            worker.log_level.should be(Logger::DEBUG)
            stub.as_null_object
          end

          Worker.start
        end
      end
    end
  end
end
