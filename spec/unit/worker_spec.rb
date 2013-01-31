require 'spec_helper'
require 'yaml'
require 'qless/worker'

module Qless
  describe Worker do
    let(:reserver) { fire_double("Qless::JobReservers::Ordered", description: "job reserver") }
    let(:client  ) { stub.as_null_object }

    def procline
      $0
    end

    class FileWriterJob
      def self.perform(job)
        sleep(job['sleep']) if job['sleep']
        File.open(job['file'], "w") { |f| f.write("done: #{$0}") }
      end
    end

    let(:job_output_file) { File.join(temp_dir, "job.out.#{Time.now.to_i}") }
    let(:log_output) { StringIO.new }
    let(:job) { Job.build(client, FileWriterJob, data: { 'file' => job_output_file }) }

    let(:temp_dir) { "./spec/tmp" }
    before do
      FileUtils.rm_rf temp_dir
      FileUtils.mkdir_p temp_dir
      reserver.stub(:reserve).and_return(job, nil)
    end

    def middleware_module(num)
      Module.new {
        define_method :around_perform do |job|
          File.open(job['file'] + ".before#{num}", 'w') { |f| f.write("before#{num}") }
          super(job)
          File.open(job['file'] + ".after#{num}", 'w') { |f| f.write("after#{num}") }
        end
      }
    end

    shared_examples_for 'a working worker' do
      describe "#perform" do
        class MyJobClass; end
        let(:job) { Job.build(client, MyJobClass) }

        it 'performs the job' do
          MyJobClass.should_receive(:perform)
          worker.perform(job)
        end

        it 'fails the job if performing it raises an error, including root exceptions' do
          MyJobClass.stub(:perform) { raise Exception.new("boom") }
          expected_line_number = __LINE__ - 1
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            group.should eq("Qless::MyJobClass:Exception")
            message.should include("boom")
            message.should include("#{__FILE__}:#{expected_line_number}")
          end

          worker.perform(job)
        end

        it 'fails the job if performing it raises a non-retryable error' do
          MyJobClass.stub(:retryable_exception_classes).and_return([])
          MyJobClass.stub(:perform) { raise Exception.new("boom") }
          expected_line_number = __LINE__ - 1
          job.should respond_to(:fail).with(2).arguments

          job.should_receive(:fail) do |group, message|
            group.should eq("Qless::MyJobClass:Exception")
            message.should include("boom")
            message.should include("#{__FILE__}:#{expected_line_number}")
          end

          worker.perform(job)
        end

        it 'retries the job if performing it raises a retryable error' do
          MyJobClass.stub(:retryable_exception_classes).and_return([ArgumentError])
          MyJobClass.stub(:perform) { raise ArgumentError.new("boom") }

          job.should_receive(:retry).with(no_args)

          worker.perform(job)
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

        it 'does not complete the job if the job logic itself changes the state of it (e.g. moves it to a new queue)' do
          MyJobClass.stub(:perform) { |j| j.move("other") }
          job.should_not_receive(:complete)
          worker.perform(job)
        end
      end

      describe "#work" do
        around(:each) do |example|
          old_procline = procline
          example.run
          $0 = old_procline
        end

        it "performs the job in a process and it completes" do
          worker.work(0)
          File.read(job_output_file).should include("done")
        end

        it 'supports middleware modules' do
          worker.extend middleware_module(1)
          worker.extend middleware_module(2)

          worker.work(0)
          File.read(job_output_file + '.before1').should eq("before1")
          File.read(job_output_file + '.after1').should eq("after1")
          File.read(job_output_file + '.before2').should eq("before2")
          File.read(job_output_file + '.after2').should eq("after2")
        end

        it 'fails the job if a middleware module raises an error' do
          expected_line_number = __LINE__ + 3
          worker.extend Module.new {
            def around_perform(job)
              raise "boom"
              super(job)
            end
          }

          job.should respond_to(:fail).with(2).arguments
          job.should_receive(:fail) do |group, message|
            message.should include("boom")
            message.should include("#{__FILE__}:#{expected_line_number}")
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
          starting_procline.should include("Starting")
        end

        it 'can be unpaused' do
          worker.pause_processing

          paused_checks = 0
          old_paused = worker.method(:paused?)
          worker.stub(:paused?) do
            paused_checks += 1 # count the number of loop iterations
            worker.unpause_processing if paused_checks == 20 # so we don't loop forever
            old_paused.call
          end

          worker.work(0)
          paused_checks.should be >= 20
        end
      end
    end

    context 'multi process' do
      let(:worker) { Worker.new(reserver, output: log_output, verbose: true) }
      it_behaves_like 'a working worker'
      after { worker.send :kill_child }

      it 'sets an appropriate procline for the parent process' do
        parent_procline = nil
        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          parent_procline = procline
          old_wait.call(child)
        end

        worker.work(0)
        parent_procline.should match(/Forked .* at/)
      end

      it 'sets an appropriate procline in the child process' do
        worker.work(0)
        output = File.read(job_output_file)
        output.should include("Processing", job.queue_name, job.klass_name, job.jid)
      end

      it 'kills the child immediately when told to #shutdown!' do
        job['sleep'] = 0.5 # to ensure the parent has a chance to kill the child before it does work

        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          worker.shutdown!
          old_wait.call(child)
        end

        File.exist?(job_output_file).should be_false
        worker.work(0)
        File.exist?(job_output_file).should be_false
      end

      it 'stops working when told to shutdown' do
        num_jobs_performed = 0
        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          worker.shutdown if num_jobs_performed == 1
          num_jobs_performed += 1
          old_wait.call(child)
        end

        reserver.stub(:reserve).and_return(job, job)
        worker.work(0.0001)
        num_jobs_performed.should eq(2)
      end

      it 'can be paused' do
        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          worker.pause_processing # pause the worker after starting the first job
          old_wait.call(child)
        end

        paused_procline = nil
        paused_checks = 0
        old_paused = worker.method(:paused?)
        worker.stub(:paused?) do
          paused_checks += 1 # count the number of loop iterations
          worker.shutdown if paused_checks == 20 # so we don't loop forever
          old_paused.call.tap do |paused|
            paused_procline = procline if paused
          end
        end

        # a job should only be reserved once because it is paused while processing the first one
        reserver.should_receive(:reserve).once
        worker.work(0)
        paused_checks.should eq(20)
        paused_procline.should include("Paused")
      end
    end

    context 'single process' do
      let(:worker) { Worker.new(reserver, run_as_single_process: '1', output: log_output, verbose: true) }
      it_behaves_like 'a working worker'

      it 'stops working when told to shutdown' do
        num_jobs_performed = 0
        old_perform = worker.method(:perform)
        worker.stub(:perform) do |job|
          worker.shutdown if num_jobs_performed == 1
          num_jobs_performed += 1
          old_perform.call(job)
        end

        reserver.stub(:reserve).and_return(job, job)
        worker.work(0.0001)
        num_jobs_performed.should eq(2)
      end

      it 'can be paused' do
        old_perform = worker.method(:perform)
        worker.stub(:perform) do |job|
          worker.pause_processing # pause the worker after starting the first job
          old_perform.call(job)
        end

        paused_procline = nil
        paused_checks = 0
        old_paused = worker.method(:paused?)
        worker.stub(:paused?) do
          paused_checks += 1 # count the number of loop iterations
          worker.shutdown if paused_checks == 20 # so we don't loop forever
          old_paused.call.tap do |paused|
            paused_procline = procline if paused
          end
        end

        # a job should only be reserved once because it is paused while processing the first one
        reserver.should_receive(:reserve).once
        worker.work(0)
        paused_checks.should eq(20)
        paused_procline.should include("Paused")
      end

      context "when completing the job fails" do
        it 'logs the fact and does not kill the worker' do
          job.stub(:complete).and_raise(Qless::Job::CantCompleteError)
          worker.work(0)
          log_output.string.should include("CantCompleteError")
        end
      end
    end

    describe ".start" do
      def with_env_vars(vars = {})
        defaults = {
          'REDIS_URL' => redis_url,
          'QUEUE' => 'mock_queue'
        }
        super(defaults.merge(vars)) { yield }
      end

      it 'starts working with sleep interval INTERVAL' do
        with_env_vars "INTERVAL" => "2.3" do
          worker = fire_double("Qless::Worker")
          Worker.stub(:new).and_return(worker)
          worker.should_receive(:work).with(2.3)

          Worker.start
        end
      end

      it 'defaults the sleep interval to 5.0' do
        with_env_vars do
          worker = fire_double("Qless::Worker")
          Worker.stub(:new).and_return(worker)
          worker.should_receive(:work).with(5.0)

          Worker.start
        end
      end

      it 'uses the named QUEUE' do
        with_env_vars 'QUEUE' => 'normal' do
          Worker.should_receive(:new) do |reserver|
            reserver.queues.map(&:name).should eq(["normal"])
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'uses the named QUEUES (comma delimited)' do
        with_env_vars 'QUEUES' => 'high,normal, low' do
          Worker.should_receive(:new) do |reserver|
            reserver.queues.map(&:name).should eq(["high", "normal", "low"])
            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'raises an error if no queues are provided' do
        with_env_vars 'QUEUE' => '', 'QUEUES' => '' do
          expect {
            Worker.start
          }.to raise_error(/must pass QUEUE or QUEUES/)
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

      it 'sets verbose, very_verbose, run_as_single_process to false by default' do
        with_env_vars do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver, options.merge(:output => StringIO.new))
            worker.verbose.should be_false
            worker.very_verbose.should be_false
            worker.run_as_single_process.should be_false

            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets verbose=true when passed VERBOSE' do
        with_env_vars 'VERBOSE' => '1' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver, options.merge(:output => StringIO.new))
            worker.verbose.should be_true
            worker.very_verbose.should be_false
            worker.run_as_single_process.should be_false

            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets very_verbose=true when passed VVERBOSE' do
        with_env_vars 'VVERBOSE' => '1' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver, options.merge(:output => StringIO.new))
            worker.verbose.should be_false
            worker.very_verbose.should be_true
            worker.run_as_single_process.should be_false

            stub.as_null_object
          end

          Worker.start
        end
      end

      it 'sets run_as_single_process=true when passed RUN_AS_SINGLE_PROCESS' do
        with_env_vars 'RUN_AS_SINGLE_PROCESS' => '1' do
          orig_new = Worker.method(:new)
          Worker.should_receive(:new) do |reserver, options = {}|
            worker = orig_new.call(reserver, options)
            worker.verbose.should be_false
            worker.very_verbose.should be_false
            worker.run_as_single_process.should be_true

            stub.as_null_object
          end

          Worker.start
        end
      end
    end
  end
end

