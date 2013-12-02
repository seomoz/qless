# Encoding: utf-8

require 'qless/job_reservers/round_robin'

module Qless
  module JobReservers
    # Like round-robin but shuffles the order of the queues
    class ShuffledRoundRobin < RoundRobin
      def initialize(queues)
        super(queues.shuffle)
      end

      def prep_for_work!
        @queues = @queues.shuffle
        reset_description!
      end

      TYPE_DESCRIPTION = 'shuffled round robin'
    end
  end
end
