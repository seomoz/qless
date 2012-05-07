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
  end
end

