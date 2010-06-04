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

  def test_differing_schemas
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

    file = File.open('data.avr', 'wb')
    schema = Avro::Schema.parse(writer_schema)
    writer = Avro::IO::DatumWriter.new(schema)
    dw = Avro::DataFile::Writer.new(file, writer, schema)
    data.each{|h| dw << h }
    dw.close

    file = File.open('data.avr', 'r+')
    dr = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)

    # extract the username only from the avro serialized file
    reader_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"}
 ]}
JSON

    reader = Avro::IO::DatumReader.new(nil, Avro::Schema.parse(reader_schema))
    dr = Avro::DataFile::Reader.new(file, reader)
    run = false
    dr.each_with_index do |record, i|
      run = true
      assert_equal data[i]['username'], record['username']
    end
    assert run, "enumerable is run through at least once"
  end
end
