require 'spec_helper'
require 'qless/middleware/memory_usage_monitor'

module Qless
  module Middleware
    describe MemoryUsageMonitor do
      mem_usage_from_other_technique = nil

      shared_examples_for "memory usage monitor" do
        it 'can report the amount of memory the process is using' do
          mem = MemoryUsageMonitor.current_usage

          # We expect mem usage to be at least 10MB, but less than 10GB
          expect(mem).to be > 10_000_000
          expect(mem).to be < 10_000_000_000

          if mem_usage_from_other_technique
            expect(mem).to be_within(10).percent_of(mem_usage_from_other_technique)
          else
            mem_usage_from_other_technique = mem
          end
        end
      end

      context "when the proc-wait3 gem is available" do
        before do
          load "qless/middleware/memory_usage_monitor.rb"

          unless Process.respond_to?(:getrusage)
            pending "Could not load the proc-wait3 gem"
          end
        end

        include_examples "memory usage monitor"
      end

      context "when the proc-wait3 gem is not available" do
        before do
          MemoryUsageMonitor.stub(:warn)
          MemoryUsageMonitor.stub(:require).and_raise(LoadError)
          load "qless/middleware/memory_usage_monitor.rb"
        end

        include_examples "memory usage monitor"
      end
    end
  end
end

