require 'qless'
require 'qless/test_helpers/worker_helpers'
require 'qless/worker'
require 'qless/job_reservers/round_robin'
require 'tempfile'

class TempfileWithString < Tempfile
  # To mirror StringIO#string
  def string
    rewind
    read.tap { close }
  end
end

shared_context "forking worker" do
  include Qless::WorkerHelpers
  include_context "redis integration"

  let(:key) { :worker_integration_job }
  let(:queue) { client.queues['main'] }
  let(:log_io) { TempfileWithString.new('qless.log') }
  let(:worker) do
    Qless::Workers::ForkingWorker.new(
      Qless::JobReservers::RoundRobin.new([queue]),
      interval: 1,
      max_startup_interval: 0,
      output: log_io,
      log_level: Logger::DEBUG)
  end

  after { log_io.unlink }
end

