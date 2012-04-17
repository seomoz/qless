require 'spec_helper'
require 'qless/job'

module Qless
  describe Job do
    class JobClass
      class Nested
      end
    end

    let(:client) { stub.as_null_object }

    describe ".mock" do
      it 'creates a job instance' do
        Job.mock(client, JobClass).should be_a(Job)
      end

      it 'honors attributes passed as a symbol' do
        job = Job.mock(client, JobClass, data: { "a" => 5 })
        job.data.should eq("a" => 5)
      end
    end

    describe "#perform" do
      it 'calls the #perform method on the job class with the job as an argument' do
        job = Job.mock(client, JobClass)
        JobClass.should_receive(:perform).with(job).once
        job.perform
      end

      it 'properly finds nested classes' do
        job = Job.mock(client, JobClass::Nested)
        JobClass::Nested.should_receive(:perform).with(job).once
        job.perform
      end
    end
  end
end

