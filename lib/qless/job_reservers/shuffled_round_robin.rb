require 'qless/job_reservers/round_robin'

module Qless
  module JobReservers
    class ShuffledRoundRobin < RoundRobin
      def initialize(queues)
        super(queues.shuffle)
      end

      def prep_for_work!
        @queues = @queues.shuffle
      end

      TYPE_DESCRIPTION = "shuffled round robin"
    end
  end
end
