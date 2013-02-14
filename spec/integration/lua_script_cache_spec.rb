require 'spec_helper'
require "qless"
require 'yaml'

module Qless
  describe LuaScriptCache, :integration do
    def ensure_put_script_loaded(client)
      client.queues["q1"].put(Qless::Job, {})
    end

    let(:redis_1)  { Redis.new(url: redis_url) }
    let(:redis_2)  { Redis.new(url: redis_url) }
    let(:client_1) { Qless::Client.new(redis: redis_1) }
    let(:client_2) { Qless::Client.new(redis: redis_2) }

    before { stub_const("SomeJob", Class.new) }

    it 'does not prevent watch-multi-exec blocks from working properly' do
      ensure_put_script_loaded(client_1)
      ensure_put_script_loaded(client_2)

      redis_2.watch("some_key") do
        response = redis_2.multi do
          redis_1.set("some_key", "some_value") # force the multi block to no-op
          client_2.queues["some_job"].put(SomeJob, {})
        end

        expect(response).to be_nil
      end

      expect(client_2.queues["some_job"].length).to eq(0)
    end
  end
end

