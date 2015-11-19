# -*- encoding: utf-8 -*-
# stub: avro 1.9.0.pre1 ruby lib

Gem::Specification.new do |s|
  s.name = "avro"
  s.version = "1.9.0.pre1"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.2") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Apache Software Foundation"]
  s.date = "2015-11-18"
  s.description = "Avro is a data serialization and RPC format"
  s.email = "dev@avro.apache.org"
  s.extra_rdoc_files = ["CHANGELOG", "LICENSE", "lib/avro.rb", "lib/avro/data_file.rb", "lib/avro/io.rb", "lib/avro/ipc.rb", "lib/avro/protocol.rb", "lib/avro/schema.rb"]
  s.files = ["CHANGELOG", "LICENSE", "Manifest", "NOTICE", "Rakefile", "avro.gemspec", "interop/test_interop.rb", "lib/avro.rb", "lib/avro/data_file.rb", "lib/avro/io.rb", "lib/avro/ipc.rb", "lib/avro/protocol.rb", "lib/avro/schema.rb", "test/random_data.rb", "test/sample_ipc_client.rb", "test/sample_ipc_http_client.rb", "test/sample_ipc_http_server.rb", "test/sample_ipc_server.rb", "test/test_datafile.rb", "test/test_fingerprints.rb", "test/test_help.rb", "test/test_io.rb", "test/test_json_io.rb", "test/test_protocol.rb", "test/test_schema.rb", "test/test_schema_normalization.rb", "test/test_socket_transport.rb", "test/tool.rb"]
  s.homepage = "http://avro.apache.org/"
  s.licenses = ["Apache License 2.0 (Apache-2.0)"]
  s.rdoc_options = ["--line-numbers", "--title", "Avro"]
  s.rubyforge_project = "avro"
  s.rubygems_version = "2.2.2"
  s.summary = "Apache Avro for Ruby"
  s.test_files = ["test/test_datafile.rb", "test/test_fingerprints.rb", "test/test_help.rb", "test/test_io.rb", "test/test_json_io.rb", "test/test_protocol.rb", "test/test_schema.rb", "test/test_schema_normalization.rb", "test/test_socket_transport.rb"]

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<multi_json>, [">= 0"])
    else
      s.add_dependency(%q<multi_json>, [">= 0"])
    end
  else
    s.add_dependency(%q<multi_json>, [">= 0"])
  end
end
