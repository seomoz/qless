require 'benchmark'

n = 1000

Benchmark.bmbm do |x|
  x.report("shelling out") do
    n.times { Integer(`ps -o rss= -p #{Process.pid}`) * 1024 }
  end

  x.report("rusage") do
    require 'rusage'
    n.times { Process.rusage }
  end
end

=begin
On my computer:

Rehearsal ------------------------------------------------
shelling out   0.090000   0.550000   3.650000 (  4.051986)
rusage         0.010000   0.000000   0.010000 (  0.002501)
--------------------------------------- total: 3.660000sec

                   user     system      total        real
shelling out   0.100000   0.550000   3.660000 (  4.031830)
rusage         0.010000   0.000000   0.010000 (  0.001669)
=end

