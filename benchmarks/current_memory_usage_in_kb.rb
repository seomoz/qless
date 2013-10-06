$LOAD_PATH.unshift "./lib"
require 'benchmark'
require 'qless'

n = 1000

Benchmark.bm do |x|
  x.report do
    n.times { Qless.current_memory_usage_in_kb }
  end
end


=begin
On my computer:

     user     system      total        real
 0.100000   0.620000   3.400000 (  3.779580)
=end

