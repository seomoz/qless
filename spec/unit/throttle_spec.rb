# Encoding: utf-8

require 'spec_helper'
require 'yaml'
require 'qless/queue'

module Qless
  describe Throttle, :integration do
    it "stores the correct the name and client at initialization" do
      t = Throttle.new('name', client)
      t.name.should eq('name')
      t.client.should eq(client)
    end

    it "can delete the named throttle" do
      t = Throttle.new('name', client)
      t.maximum = 5
      t.maximum.should eq(5)
      t.delete
      t.maximum.should eq(0)
    end

    it "returns the throttle name when id is called" do
      t = Throttle.new('name', client)
      t.id.should eq(t.name)
    end

    it "returns the set of locked jids" do
      t = Throttle.new('name', client)
      Redis.current.zadd('ql:t:name-locks', [[1, 1], [1, 2], [1, 3]])
      t.locks.should eq(["1", "2", "3"])
    end

    it "can set and retrieve the throttle's maximum lock count" do
      t = Throttle.new('name', client)
      t.maximum = 5
      t.maximum.should eq(5)
    end

    it "returns the set of pending jids" do
      t = Throttle.new('name', client)
      Redis.current.zadd('ql:t:name-pending', [[1, 1], [1, 2], [1, 3]])
      t.pending.should eq(["1", "2", "3"])
    end

    it "handles throttle names as a String or Symbol" do
      t = Throttle.new('name', client)
      t.maximum = 5
      t.id.should eq(t.name)
    end
  end
end
