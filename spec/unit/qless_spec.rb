require 'spec_helper'
require 'qless'

describe Qless do
  describe ".generate_jid" do
    it "generates a UUID suitable for use as a jid" do
      Qless.generate_jid.should match(/\A[a-f0-9]{32}\z/)
    end
  end

  describe ".worker_name" do
    it 'includes the hostname in the worker name' do
      Qless.worker_name.should include(Socket.gethostname)
    end

    it 'includes the pid in the worker name' do
      Qless.worker_name.should include(Process.pid.to_s)
    end
  end
end

