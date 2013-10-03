# Encoding: utf-8

require 'metriks'

module Qless
  module Middleware
    module Metriks

      # Tracks the time jobs take, grouping the timings by the job class.
      module TimeJobsByClass
        def around_perform(job)
          ::Metriks.timer("qless.job-times.#{job.klass_name}").time do
            super
          end
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
              return unless job.state == 'complete'
              return unless event_name = class_to_event_map[job.klass]

              counter = ::Metriks.counter("qless.job-events.#{event_name}")
              counter.increment
            end
          end
        end
      end
    end
  end
end
