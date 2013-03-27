require 'spec_helper'
require 'qless/job'

module Qless
  describe Job do
    class JobClass
      class Nested
      end
    end

    module SomeJobMiddleware
      def around_perform(job)
        super
      end
    end

    let(:client) { stub.as_null_object }

    describe ".build" do
      it 'creates a job instance' do
        Job.build(client, JobClass).should be_a(Job)
      end

      it 'honors attributes passed as a symbol' do
        job = Job.build(client, JobClass, data: { "a" => 5 })
        job.data.should eq("a" => 5)
      end

      it 'round-trips the data through JSON to simulate what happens with real jobs' do
        time = Time.new(2012, 5, 3, 12, 30)
        job = Job.build(client, JobClass, data: { :a => 5, :timestamp => time })
        job.data.keys.should =~ %w[ a timestamp ]
        job.data["timestamp"].should be_a(String)
        job.data["timestamp"].should include("2012-05-03")
      end
    end

    describe "#klass" do
      it 'returns the class constant' do
        job = Job.build(client, JobClass, data: {})
        expect(job.klass).to be(JobClass)
      end

      it 'raises a useful error when the class constant is not loaded' do
        stub_const("MyJobClass", Class.new)
        job = Job.build(client, ::MyJobClass, data: {})
        hide_const("MyJobClass")
        expect { job.klass }.to raise_error(NameError, /constant MyJobClass/)
      end
    end

    describe "#perform" do
      it 'calls the #perform method on the job class with the job as an argument' do
        job = Job.build(client, JobClass)
        JobClass.should_receive(:perform).with(job).once
        job.perform
      end

      it 'properly finds nested classes' do
        job = Job.build(client, JobClass::Nested)
        JobClass::Nested.should_receive(:perform).with(job).once
        job.perform
      end

      context 'when the job class is a Qless::Job::SupportsMiddleware' do
        it 'calls #around_perform on the job in order to run the middleware chain' do
          klass = Class.new { extend Qless::Job::SupportsMiddleware }
          stub_const("MyJobClass", klass)

          job = Job.build(client, klass)
          klass.should_receive(:around_perform).with(job).once
          job.perform
        end
      end

      context 'when the job mixes in a middleware but has forgotten Qless::Job::SupportsMiddleware' do
        it 'raises an error to alert the user to the fact they need Qless::Job::SupportsMiddleware' do
          klass = Class.new { extend SomeJobMiddleware }
          stub_const('MyJobClass', klass)
          job = Job.build(client, klass)

          expect {
            job.perform
          }.to raise_error(Qless::Job::MiddlewareMisconfiguredError)
        end
      end
    end

    describe "#middlewares_on" do
      it 'returns the list of middleware mixed into the job' do
        klass = Class.new do
          extend Qless::Job::SupportsMiddleware
          extend SomeJobMiddleware
        end

        expect(Qless::Job.middlewares_on(klass)).to eq([
          SomeJobMiddleware, Qless::Job::SupportsMiddleware
        ])
      end
    end

    [
     [:fail, 'group', 'message'],
     [:complete],
     [:cancel],
     [:move, 'queue'],
     [:retry],
     [:retry, 55]
    ].each do |meth, *args|
      describe "##{meth}" do
        let(:job) { Job.build(client, JobClass) }

        it 'updates #state_changed? from false to true' do
          expect {
            job.send(meth, *args)
          }.to change(job, :state_changed?).from(false).to(true)
        end

        class MyCustomError < StandardError; end
        it 'does not update #state_changed? if there is a redis connection error' do
          client.stub(:"_#{meth}") { raise MyCustomError, "boom" }
          client.stub(:"_put") { raise MyCustomError, "boom" } # for #move

          expect {
            job.send(meth, *args)
          }.to raise_error(MyCustomError)
          job.state_changed?.should be_false
        end
      end
    end

    describe "#to_hash" do
      let(:job) { Job.build(client, JobClass) }

      it "prints out the state of the job" do
        hash = job.to_hash
        hash[:klass_name].should eq("Qless::JobClass")
        hash[:state].should eq("running")
      end
    end

    describe "#inspect" do
      let(:job) { Job.build(client, JobClass) }

      it "includes the jid" do
        job.inspect.should include(job.jid)
      end

      it "includes the job class" do
        job.inspect.should include(job.klass_name)
      end

      it "includes the job queue" do
        job.inspect.should include(job.queue_name)
      end
    end
  end
end

