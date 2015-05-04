$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'irb/completion'

QLESS_CONSOLE = true

require 'qless'

module StdoutLogger
  def logger
    @logger ||= Logger.new($stdout)
  end
end

# Load everything!
Dir["./lib/**/*.rb"].sort.each do |f|
  require f.gsub("./lib/", "")
end

require 'pp'
