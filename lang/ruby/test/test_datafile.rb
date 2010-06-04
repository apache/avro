require 'test_help'

class TestDataFile < Test::Unit::TestCase
  HERE = File.expand_path File.dirname(__FILE__)
  def setup
    if File.exists?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def teardown
    if File.exists?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def test_differing_schemas_with_primitives
    writer_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "verified", "type": "boolean", "default": "false"}
  ]}
JSON

    data = [{"username" => "john", "age" => 25, "verified" => true},
            {"username" => "ryan", "age" => 23, "verified" => false}]

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      data.each{|h| dw << h }
    end

    # extract the username only from the avro serialized file
    reader_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"}
 ]}
JSON

    Avro::DataFile.open('data.avr', 'r', reader_schema) do |dr|
      dr.each_with_index do |record, i|
        assert_equal data[i]['username'], record['username']
      end
    end
  end

end
