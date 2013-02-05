require 'spec_helper'
require 'yaml'
require 'qless/queue'

class SomeJobClassWithDifferentToS
  def self.to_s
    "this is a different to_s"
  end
end

module Qless
  describe Queue, :integration do
    ["name", :name].each do |name|
      it "can query the length when initialized with #{name.inspect} as the name" do
        q = Queue.new(name, client)
        q.length.should eq(0)
      end
    end

    [:to_s, :inspect].each do |meth|
      it "returns a human-readable string from ##{meth}" do
        q = Queue.new("queue-name", client)
        string = q.send(meth)
        string.should have_at_most(100).characters
        string.should include("queue-name")
      end
    end

    it "can specify a jid in put and recur" do
      client.queues['foo'].put(  Qless::Job, {'foo' => 'bar'},    :jid => 'howdy').should eq('howdy')
      client.queues['foo'].recur(Qless::Job, {'foo' => 'bar'}, 5, :jid => 'hello').should eq('hello')
      client.jobs['howdy'].should be
      client.jobs['hello'].should be
    end

    shared_examples_for "job options" do
      let(:q) { Queue.new("q", client) }

      let(:klass1) do
        Class.new do
          def self.default_job_options(data)
            { :jid => "jid-#{data[:arg]}", :priority => 100 }
          end
        end
      end

      let(:klass2) { Class.new }

      it "uses options provided by the klass's .defualt_job_options method" do
        jid = enqueue(q, klass1, { :arg => "foo" })
        job = client.jobs[jid]
        job.jid.should eq("jid-foo")
        job.priority.should eq(100)
      end

      it "overrides the default options with the passed options" do
        jid = enqueue(q, klass1, { :arg => "foo" }, :priority => 15)
        job = client.jobs[jid]
        job.priority.should eq(15)
      end

      it "works fine when the klass does not define .default_job_options" do
        jid = enqueue(q, klass2, { :arg => "foo" }, :priority => 15)
        job = client.jobs[jid]
        job.priority.should eq(15)
      end
    end

    describe "#put" do
      def enqueue(q, klass, data, opts = {})
        q.put(klass, data, opts)
      end

      include_examples "job options"

      it "uses the class's name properly (not #to_s)" do
        q = Queue.new("q", client)
        jid = enqueue(q, SomeJobClassWithDifferentToS, {})
        job = client.jobs[jid]
        job.klass_name.should eq("SomeJobClassWithDifferentToS")
      end
    end

    describe "#recur" do
      def enqueue(q, klass, data, opts = {})
        q.recur(klass, data, 10, opts)
      end

      include_examples "job options"
    end
  end
end

