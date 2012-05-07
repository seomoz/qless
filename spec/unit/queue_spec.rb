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
  end
end

