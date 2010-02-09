require 'rubygems'
require 'test/unit'
require 'avro'

class TestInterop < Test::Unit::TestCase
  HERE = File.expand_path(File.dirname(__FILE__))
  SHARE = HERE + '/../../../share'
  SCHEMAS = SHARE + '/test/schemas'
  Dir[HERE + '/../../../build/interop/data/*'].each do |fn|  
    define_method("test_read_#{File.basename(fn, 'avro')}") do
      projection = Avro::Schema.parse(File.read(SCHEMAS+'/interop.avsc'))

      File.open(fn) do |f|
        r = Avro::DataFile::Reader.new(f, Avro::IO::DatumReader.new(projection))
        i = 0
        r.each do |datum|
          i += 1
          assert_not_nil datum, "nil datum from #{fn}"
        end
        assert_not_equal 0, i, "no data read in from #{fn}"
      end
    end
  end
end
