require 'spec_helper'
require 'qless/worker'
require 'qless/job_reservers/ordered'

module Qless
  describe Worker do
    let(:reserver) { fire_double("Qless::JobReservers::Ordered", description: "job reserver") }
    let(:client) { stub.as_null_object }
    let(:worker) { Worker.new(client, reserver) }

    describe "#perform" do
      class MyJobClass; end
      let(:job) { Job.mock(client, MyJobClass) }

      it 'performs the job' do
        MyJobClass.should_receive(:perform)
        worker.perform(job)
      end

      it 'fails the job if performing it raises an error' do
        MyJobClass.stub(:perform) { raise StandardError.new("boom") }
        expected_line_number = __LINE__ - 1
        job.should respond_to(:fail).with(2).arguments

        job.should_receive(:fail) do |group, message|
          group.should eq("Qless::MyJobClass:StandardError")
          message.should include("boom")
          message.should include("#{__FILE__}:#{expected_line_number}")
        end

        worker.perform(job)
      end

      it 'completes the job if it finishes with no errors' do
        MyJobClass.stub(:perform)
        job.should respond_to(:complete).with(0).arguments
        job.should_receive(:complete).with(no_args)
        worker.perform(job)
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

      def procline
        $0
      end

      class FileWriterJob
        def self.perform(job)
          sleep(job['sleep']) if job['sleep']
          File.open(job['file'], "w") { |f| f.write("done: #{$0}") }
        end
      end

      let(:output_file) { File.join(temp_dir, "job.out.#{Time.now.to_i}") }
      let(:job) { Job.mock(client, FileWriterJob, data: { 'file' => output_file }) }

      let(:temp_dir) { "./spec/tmp" }
      before do
        FileUtils.rm_rf temp_dir
        FileUtils.mkdir_p temp_dir
        reserver.stub(:reserve).and_return(job, nil)
      end

      it "performs the job in a child process and waits for it to complete" do
        worker.work(0)
        File.read(output_file).should include("done")
      end

      it 'begins with a "starting" procline' do
        starting_procline = nil
        reserver.stub(:reserve) do
          starting_procline = procline
          nil
        end

        worker.work(0)
        starting_procline.should include("Qless: Starting")
      end

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
        output = File.read(output_file)
        output.should include("Processing", job.queue, job.klass, job.jid)
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

      it 'kills the child immediately when told to #shutdown!' do
        job['sleep'] = 0.5 # to ensure the parent has a chance to kill the child before it does work

        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          worker.shutdown!
          old_wait.call(child)
        end

        File.exist?(output_file).should be_false
        worker.work(0)
        File.exist?(output_file).should be_false
      end

      it 'can be paused' do
        old_wait = Process.method(:wait)
        Process.stub(:wait) do |child|
          worker.pause_processing # pause the worker after starting the first job
          old_wait.call(child)
        end

        paused_checks = 0
        old_paused = worker.method(:paused?)
        worker.stub(:paused?) do
          paused_checks += 1 # count the number of loop iterations
          worker.shutdown if paused_checks == 20 # so we don't loop forever
          old_paused.call
        end

        # a job should only be reserved once because it is paused while processing the first one
        reserver.should_receive(:reserve).once { job }

        worker.work(0)
        paused_checks.should eq(20)
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
end

