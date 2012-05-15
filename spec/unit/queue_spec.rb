require 'spec_helper'
require 'yaml'
require 'qless/queue'

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
  end
end

