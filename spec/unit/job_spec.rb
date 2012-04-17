require 'spec_helper'
require 'qless/job'

module Qless
  describe Job do
    class JobClass; end

    describe ".mock" do
      let(:client) { stub.as_null_object }

      it 'creates a job instance' do
        Job.mock(client, JobClass).should be_a(Job)
      end

      it 'honors attributes passed as a symbol' do
        job = Job.mock(client, JobClass, data: { "a" => 5 })
        job.data.should eq("a" => 5)
      end
    end
  end
end

