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

sub codec_file {
    my ($codec, $payload) = @_;
    my $fh = File::Temp->new(UNLINK => 1);
    my $writer = Avro::DataFileWriter->new(
        fh            => $fh,
        writer_schema => $schema,
        codec         => $codec,
    );
    $writer->print($payload);
    $writer->flush;
    seek $fh, 0, 0;
    return $fh;
}

## A large, highly compressible value compresses to a tiny block but would
## decompress to far more than the configured limit; reading it must be rejected
## for every codec. The limit is set via AVRO_MAX_DECOMPRESS_LENGTH.
sub assert_codec_rejects_oversized {
    my ($codec) = @_;
    my $big = "a" x (64 * 1024); # 64 KiB
    my $fh = codec_file($codec, $big);
    local $ENV{AVRO_MAX_DECOMPRESS_LENGTH} = 1024;
    my $reader = Avro::DataFileReader->new(
        fh            => $fh,
        reader_schema => $schema,
    );
    throws_ok { $reader->all }
        'Avro::DataFile::Error::DecompressionSize',
        "$codec block exceeding the limit is rejected";
}

assert_codec_rejects_oversized('deflate');

SKIP: {
    eval { require IO::Compress::Bzip2; 1 }
        or skip 'IO::Compress::Bzip2 not available', 1;
    assert_codec_rejects_oversized('bzip2');
}

SKIP: {
    eval { require Compress::Zstd::Decompressor; 1 }
        or skip 'Compress::Zstd::Decompressor not available', 1;
    assert_codec_rejects_oversized('zstandard');
}

## A block within the limit still decodes correctly.
sub assert_codec_within_limit_decodes {
    my ($codec) = @_;
    my $payload = "hello world";
    my $fh = codec_file($codec, $payload);
    local $ENV{AVRO_MAX_DECOMPRESS_LENGTH} = 1024 * 1024;
    my $reader = Avro::DataFileReader->new(
        fh            => $fh,
        reader_schema => $schema,
    );
    my @all = $reader->all;
    is_deeply \@all, [$payload], "$codec block within the limit decodes";
}

assert_codec_within_limit_decodes('deflate');

SKIP: {
    eval { require IO::Compress::Bzip2; 1 }
        or skip 'IO::Compress::Bzip2 not available', 1;
    assert_codec_within_limit_decodes('bzip2');
}

SKIP: {
    eval { require Compress::Zstd::Decompressor; 1 }
        or skip 'Compress::Zstd::Decompressor not available', 1;
    assert_codec_within_limit_decodes('zstandard');
}

## When block_max_size is configured on the reader, a block whose declared
## compressed size exceeds it is rejected before the compressed block is read
## into memory (guarding against an attacker-controlled block_size allocation).
{
    my $payload = "a" x (32 * 1024); # 32 KiB, compresses to a few dozen bytes
    my $fh = codec_file('deflate', $payload);
    # Set a large decompressed-size cap so the rejection can only come from the
    # compressed-size guard, not the decompression limit, regardless of any
    # AVRO_MAX_DECOMPRESS_LENGTH the runner may have set.
    local $ENV{AVRO_MAX_DECOMPRESS_LENGTH} = 1024 * 1024 * 1024;
    my $reader = Avro::DataFileReader->new(
        fh             => $fh,
        reader_schema  => $schema,
        block_max_size => 8, # smaller than any real compressed block
    );
    eval { $reader->all };
    my $err = $@;
    isa_ok $err, 'Avro::DataFile::Error::CompressedBlockSize',
        'compressed block exceeding block_max_size is rejected before reading';
    like "$err", qr/block_max_size/,
        'rejection is due to the compressed-size guard, not the decompression cap';
}

done_testing;
