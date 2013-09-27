# Encoding: utf-8

require 'simplecov'

SimpleCov.start do
  add_filter '/spec'
  add_filter '/bundle'
end

SimpleCov.at_exit do
  path = File.join(SimpleCov.coverage_path, 'coverage_percent.txt')
  File.open(path, 'w') do |f|
    f.write SimpleCov.result.covered_percent
  end
  SimpleCov.result.format!
end
