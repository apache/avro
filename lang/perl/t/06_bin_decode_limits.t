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

# Decoding arrays and maps must bound the block count read from the input. The
# block count drives allocation of the resulting collection, so a pathological
# or truncated input declaring a very large block count must raise an error
# instead of attempting an unbounded allocation.

use strict;
use warnings;

# Load the decoder with its default limit regardless of the runner environment:
# $MAX_COLLECTION_ITEMS is initialized from AVRO_MAX_COLLECTION_ITEMS at load
# time, so clear it before any Avro module is loaded.
BEGIN { delete $ENV{AVRO_MAX_COLLECTION_ITEMS}; }

use Avro::Schema;
use Test::More;
use Test::Exception;

use_ok 'Avro::BinaryDecoder';

my $array_schema = Avro::Schema->parse(q({"type": "array", "items": "null"}));
my $map_schema   = Avro::Schema->parse(q({"type": "map", "values": "null"}));

sub decode_bytes {
    my ($schema, $bytes) = @_;
    open my $reader, '<', \$bytes or die "Can't open memory file: $!";
    return Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
}

my $err = 'Avro::BinaryDecoder::Error::CollectionSize';

{
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 10;

    # zigzag(11) = 0x16: a single block of 11 items exceeds the limit of 10.
    throws_ok { decode_bytes($array_schema, "\x16\x00") } $err,
        "array block count above limit is rejected";

    throws_ok { decode_bytes($map_schema, "\x16\x00") } $err,
        "map block count above limit is rejected";

    # Two blocks of 6 items (zigzag(6) = 0x0c) exceed the limit cumulatively.
    # Well-formed encoding: two blocks of six null items (0 bytes each) followed
    # by the terminating 0 block count.
    throws_ok { decode_bytes($array_schema, "\x0c\x0c\x00") } $err,
        "array cumulative block count above limit is rejected";

    # Repeated map keys collapse in the hash; the cumulative check must still
    # count every decoded pair. Two well-formed blocks of 6 pairs all keyed "a"
    # (zigzag(6)=0x0c, key string = 0x02 0x61, null value = no bytes) then the
    # terminating 0 block count exceed the limit of 10.
    my $repeated_key_map = "\x0c" . ("\x02\x61" x 6) . "\x0c" . ("\x02\x61" x 6) . "\x00";
    throws_ok { decode_bytes($map_schema, $repeated_key_map) } $err,
        "map cumulative pair count with repeated keys is rejected";

    # Negative count: unsigned varint 0x15 decodes (zigzag) to -11, whose
    # absolute value (11) is used; a block size long (0x00) follows.
    throws_ok { decode_bytes($array_schema, "\x15\x00") } $err,
        "negative array block count is bounded by its absolute value";

    # zigzag(3) = 0x06: three null items are within the limit and decode fine.
    my $decoded = decode_bytes($array_schema, "\x06\x00");
    is_deeply $decoded, [undef, undef, undef],
        "array within the limit still decodes";
}

# By default the limit is generous enough not to affect ordinary decoding.
is $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS, $Avro::BinaryDecoder::DEFAULT_MAX_COLLECTION_ITEMS,
    "default collection item limit restored outside local scope";

done_testing;
