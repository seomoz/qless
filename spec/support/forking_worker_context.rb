require 'qless'
require 'qless/test_helpers/worker_helpers'
require 'qless/worker'
require 'qless/job_reservers/round_robin'

shared_context "forking worker" do
  include Qless::WorkerHelpers
  include_context "redis integration"

  let(:key) { :worker_integration_job }
  let(:queue) { client.queues['main'] }
  let(:worker) do
    Qless::Workers::ForkingWorker.new(
      Qless::JobReservers::RoundRobin.new([queue]),
      interval: 1,
      max_startup_interval: 0,
      log_level: Logger::DEBUG)
  end
end

