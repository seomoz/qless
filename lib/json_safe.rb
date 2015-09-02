require 'json'

module JSON
  old_parse = JSON.method(:parse)
  define_singleton_method(:parse) do |arg|
    old_parse[arg.encode("UTF-8", :invalid => :replace, :undef => :replace, :replace => "").force_encoding('UTF-8')]
  end
end
