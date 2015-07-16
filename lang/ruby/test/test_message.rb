# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'
require 'avro/message'

class TestMessage < Test::Unit::TestCase
  def test_encoding_and_decoding
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "record",
        "name": "Pageview",
        "fields" : [
          { "name": "url", "type": "string" }
        ]
      }
    JSON

    message_writer = Avro::Message::Writer.new(schema)
    message_reader = Avro::Message::Reader.new(schema)

    datum = { "url" => "http://example.com/test" }
    encoded_datum = message_writer.write(datum)
    decoded_datum = message_reader.read(encoded_datum)

    assert_equal datum, decoded_datum
  end
end
