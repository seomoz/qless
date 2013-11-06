require 'benchmark'

n = 1000

Benchmark.bmbm do |x|
  x.report("shelling out") do
    n.times { Integer(`ps -o rss= -p #{Process.pid}`) * 1024 }
  end

  x.report("proc-wait3") do
    require 'proc/wait3'
    n.times { Process.getrusage }
  end
end

=begin
On my computer:

Rehearsal ------------------------------------------------
shelling out   0.100000   0.670000   3.750000 (  4.141153)
proc-wait3     0.000000   0.000000   0.000000 (  0.002601)
--------------------------------------- total: 3.750000sec

                   user     system      total        real
shelling out   0.090000   0.730000   3.760000 (  4.150078)
proc-wait3     0.000000   0.000000   0.000000 (  0.003116)
=end

