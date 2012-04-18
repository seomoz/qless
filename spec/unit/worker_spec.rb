require 'spec_helper'
require 'qless/worker'
require 'qless/job_reservers/ordered'

module Qless
  describe Worker do
    let(:reserver) { fire_double("Qless::JobReservers::Ordered") }
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
          File.open(job['file'], "w") { |f| f.write("done: #{$0}") }
        end
      end
      let(:output_file) { File.join(temp_dir, "job.out") }
      let(:job) { Job.mock(client, FileWriterJob, data: { 'file' => output_file }) }

      let(:temp_dir) { "./spec/tmp" }
      before do
       FileUtils.mkdir_p temp_dir
       FileUtils.rm_r Dir.glob("#{temp_dir}/*")
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
    end
  end
end

