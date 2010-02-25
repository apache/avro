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

require 'rubygems'
require 'echoe'
VERSION = File.open('../../share/VERSION.txt').read
Echoe.new('avro', VERSION) do |p|
  p.author = "Jeff Hodges"
  p.author = "Ryan King"
  p.summary = "Apache Avro for Ruby"
  p.url = "http://hadoop.apache.org/avro/"
  p.runtime_dependencies = %w[rubygems yajl]
end

t = Rake::TestTask.new(:interop)
t.pattern = 'interop/test*.rb'

task :generate_interop do
  $:.unshift(HERE + '/lib')
  $:.unshift(HERE + '/test')
  require 'avro'
  require 'random_data'

  schema = Avro::Schema.parse(File.read(SCHEMAS + '/interop.avsc'))
  r = RandomData.new(schema, ENV['SEED'])
  f = File.open(BUILD + '/interop/data/ruby.avro', 'w')
  writer = Avro::DataFile::Writer.new(f, Avro::IO::DatumWriter.new(schema), schema)
  begin
    writer << r.next
    writer << r.next
  ensure
    writer.close
  end
end


HERE = File.expand_path(File.dirname(__FILE__))
SHARE = HERE + '/../../share'
SCHEMAS = SHARE + '/test/schemas'
BUILD = HERE + '/../../build'

task :dist => [:manifest, :gem] do
  mkdir_p "../../dist/ruby"
  cp "pkg/avro-#{VERSION}.gem", "../../dist/ruby"
end
