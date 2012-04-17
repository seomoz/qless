require 'spec_helper'
require 'qless'

describe Qless do
  describe ".generate_jid" do
    it "generates a UUID suitable for use as a jid" do
      Qless.generate_jid.should match(/\A[a-f0-9]{32}\z/)
    end
  end
end

