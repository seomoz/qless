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
      class FileWriterJob
        def self.perform(job)
          File.open(job['file'], "w") { |f| f.write("done") }
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
        File.read(output_file).should eq("done")
      end
    end
  end
end

