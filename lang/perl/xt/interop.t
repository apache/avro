# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use strict;
use warnings;

use Test::More;
use File::Basename qw(basename);
use IO::File;
use_ok 'Avro::DataFile';
use_ok 'Avro::DataFileReader';

my $expected_metadata = {
    stringKey => 'stringValue',
    bytesKey => 'bytesValue'
};

for my $path (glob '../../build/interop/data/*.avro') {
    my $fn = basename($path);
    substr($fn, rindex $fn, '.') = '';
    my $idx = rindex $fn, '_';
    if (-1 < $idx) {
        my $codec = substr $fn, $idx + 1;
        unless ($Avro::DataFile::ValidCodec{$codec}) {
            diag("Skipped: ${path}");
            next;
        }
    }
    my $fh = IO::File->new($path);
    my $reader = Avro::DataFileReader->new(fh => $fh);

    my $metadata = $reader->metadata;
    if (exists $metadata->{stringKey}) {
        is($metadata->{stringKey}, $expected_metadata->{stringKey}, "check user metadata: stringKey ");
    }
    if (exists $metadata->{bytesKey}) {
        is($metadata->{bytesKey}, join('', $expected_metadata->{bytesKey}), "check user metadata: bytesKey ");
    }
    diag("Succeeded: ${path}");
}

done_testing;
