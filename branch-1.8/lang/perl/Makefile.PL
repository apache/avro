# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use Config;
use inc::Module::Install;

my $version = `cat ../../share/VERSION.txt`;

license 'apache';
version $version;
readme_from 'lib/Avro.pm';
all_from 'lib/Avro.pm';
build_requires 'Test::More', 0.88;
test_requires 'Math::BigInt';
test_requires 'Test::Exception';
requires 'JSON::XS';
requires 'Try::Tiny';
requires 'parent';
requires 'Regexp::Common';
requires 'Encode';
requires 'IO::String';
requires 'Object::Tiny';
requires 'Compress::Zlib';
unless ($Config{use64bitint}) {
    requires 'Math::BigInt';
}
auto_set_repository();

WriteMakefile(PM_FILTER => "sed -e 's/\+\+MODULE_VERSION\+\+/$version/'");
