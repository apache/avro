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

#!/usr/bin/env perl

# A data-file block is decompressed according to the file's codec. A block with
# a very high compression ratio can expand to far more memory than its
# compressed size. These tests ensure that reading such a block is rejected
# instead of allocating without bound.

use strict;
use warnings;
use File::Temp;
use Avro::Schema;
use Avro::DataFileWriter;
use Test::More;
use Test::Exception;

use_ok 'Avro::DataFileReader';

my $schema = Avro::Schema->parse('"string"');

sub deflate_file {
    my ($payload) = @_;
    my $fh = File::Temp->new(UNLINK => 1);
    my $writer = Avro::DataFileWriter->new(
        fh            => $fh,
        writer_schema => $schema,
        codec         => 'deflate',
    );
    $writer->print($payload);
    $writer->flush;
    seek $fh, 0, 0;
    return $fh;
}

## A large, highly compressible value compresses to a tiny block but would
## decompress to far more than the configured limit.
{
    my $big = "a" x (64 * 1024); # 64 KiB
    my $fh = deflate_file($big);
    my $reader = Avro::DataFileReader->new(
        fh             => $fh,
        reader_schema  => $schema,
        block_max_size => 1024,
    );
    throws_ok { $reader->all }
        'Avro::DataFile::Error::DecompressionSize',
        'deflate block exceeding the limit is rejected';
}

## The AVRO_MAX_DECOMPRESS_LENGTH environment variable is honored too.
{
    my $big = "a" x (64 * 1024);
    my $fh = deflate_file($big);
    local $ENV{AVRO_MAX_DECOMPRESS_LENGTH} = 1024;
    my $reader = Avro::DataFileReader->new(
        fh            => $fh,
        reader_schema => $schema,
    );
    throws_ok { $reader->all }
        'Avro::DataFile::Error::DecompressionSize',
        'AVRO_MAX_DECOMPRESS_LENGTH is honored';
}

## A block within the limit still decodes correctly.
{
    my $payload = "hello world";
    my $fh = deflate_file($payload);
    my $reader = Avro::DataFileReader->new(
        fh             => $fh,
        reader_schema  => $schema,
        block_max_size => 1024 * 1024,
    );
    my @all = $reader->all;
    is_deeply \@all, [$payload], 'deflate block within the limit decodes';
}

done_testing;
