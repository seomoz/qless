# Encoding: utf-8

# This middleware is now a no-op because
# the metriks dependency breaks builds.

module Qless
  module Middleware
    module Metriks

      # Tracks the time jobs take, grouping the timings by the job class.
      module TimeJobsByClass
        def around_perform(job)
          super
        end
      end

      # Increments a counter each time an instance of a particular job class
      # completes.
      #
      # Usage:
      #
      # Qless::Worker.class_eval do
      #   include Qless::Middleware::CountEvents.new(
      #     SomeJobClass => "event_name",
      #     SomeOtherJobClass => "some_other_event"
      #   )
      # end
      class CountEvents < Module
        def initialize(class_to_event_map)
          module_eval do # eval the block within the module instance
            define_method :around_perform do |job|
              super(job)
            end
          end
        end
      end
    end
  end
end
