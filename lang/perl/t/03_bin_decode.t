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

use strict;
use warnings;
use Avro::Schema;
use Avro::BinaryEncoder;
use Test::More;
use Test::Exception;

use_ok 'Avro::BinaryDecoder';

## spec examples
{
    my $enc = "\x06\x66\x6f\x6f";
    my $schema = Avro::Schema->parse(q({ "type": "string" }));
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is $dec, "foo", "Binary_Encodings.Primitive_Types";

    $schema = Avro::Schema->parse(<<EOJ);
          {
          "type": "record",
          "name": "test",
          "fields" : [
          {"name": "a", "type": "long"},
          {"name": "b", "type": "string"}
          ]
          }
EOJ
    open $reader, '<', \"\x36\x06\x66\x6f\x6f" or die "Can't open memory file: $!";
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, { a => 27, b => 'foo' },
                    "Binary_Encodings.Complex_Types.Records";

    open $reader, '<', \"\x04\x06\x36\x00" or die "Can't open memory file: $!";
    $schema = Avro::Schema->parse(q({"type": "array", "items": "long"}));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, [3, 27], "Binary_Encodings.Complex_Types.Arrays";

    open $reader, '<', \"\x02" or die "Can't open memory file: $!";
    $schema = Avro::Schema->parse(q(["string","null"]));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader         => $reader,
    );
    is $dec, undef, "Binary_Encodings.Complex_Types.Unions-null";

    open $reader, '<', \"\x00\x02\x61" or die "Can't open memory file: $!";
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is $dec, "a", "Binary_Encodings.Complex_Types.Unions-a";
}

## union and enum index bounds
{
    my $union = Avro::Schema->parse(q(["string","null"]));
    # Index 2 (zig-zag int 0x04) is out of range for a 2-branch union.
    open my $reader, '<', \"\x04" or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $union,
            reader_schema => $union,
            reader        => $reader,
        );
    } 'Avro::Schema::Error::Parse', "union branch index out of range is rejected";

    # Index -1 (zig-zag int 0x01) must not wrap to a valid branch.
    open $reader, '<', \"\x01" or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $union,
            reader_schema => $union,
            reader        => $reader,
        );
    } 'Avro::Schema::Error::Parse', "negative union branch index is rejected";

    my $enum = Avro::Schema->parse(
        q({ "type": "enum", "name": "e", "symbols": [ "a", "b" ] }));
    # Index 9 (zig-zag int 0x12) is out of range for a 2-symbol enum.
    open $reader, '<', \"\x12" or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $enum,
            reader_schema => $enum,
            reader        => $reader,
        );
    } 'Avro::Schema::Error::Parse', "enum symbol index out of range is rejected";

    # Index -1 (zig-zag int 0x01) must not wrap to a valid symbol.
    open $reader, '<', \"\x01" or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $enum,
            reader_schema => $enum,
            reader        => $reader,
        );
    } 'Avro::Schema::Error::Parse', "negative enum symbol index is rejected";
}

## overlong varint
{
    # A 64-bit value uses at most 10 bytes; an 11th continuation byte is a
    # malformed overlong varint and must be rejected.
    my $long = Avro::Schema->parse(q({ "type": "long" }));
    my $data = ("\x80" x 10) . "\x01";
    open my $reader, '<', \$data or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $long,
            reader_schema => $long,
            reader        => $reader,
        );
    } 'Avro::Schema::Error::Parse', "overlong varint is rejected";
}

## enum schema resolution
{

    my $w_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "a", "b", "c", "\$", "z" ] }
EOP
    my $r_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "\$", "b", "c", "d" ] }
EOP
    ok ! !Avro::Schema->match( reader => $r_enum, writer => $w_enum ), "match";
    my $enc;
    for my $data (qw/b c $/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        open my $reader, '<', \$enc or die "Cannot open memory file: $!";
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader        => $reader,
        );
        is $dec, $data, "decoded!";
    }

    for my $data (qw/a z/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        open my $reader, '<', \$enc or die "Cannot open memory file: $!";
        throws_ok { Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader        => $reader,
        )} "Avro::Schema::Error::Mismatch", "schema problem";
    }
}

## record resolution
{
    my $w_schema = Avro::Schema->parse(<<EOJ);
          { "type": "record", "name": "test",
            "fields" : [
                {"name": "a", "type": "long"},
                {"name": "bonus", "type": "string"} ]}
EOJ

    my $r_schema = Avro::Schema->parse(<<EOJ);
          { "type": "record", "name": "test",
            "fields" : [
                {"name": "t", "type": "float", "default": 37.5 },
                {"name": "a", "type": "long"} ]}
EOJ

    my $data = { a => 1, bonus => "i" };
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader        => $reader,
    );
    is $dec->{a}, 1, "easy";
    ok ! exists $dec->{bonus}, "bonus extra field ignored";
    is $dec->{t}, 37.5, "default t from reader used";

    ## delete the default for t
    delete $r_schema->fields->[0]{default};
    open $reader, '<', \$enc or die "Cannot open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader        => $reader,
        );
    } "Avro::Schema::Error::Mismatch", "no default value!";
}

## union resolution
{
    my $w_schema = Avro::Schema->parse(<<EOP);
[ "string", "null", { "type": "array", "items": "long" }]
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
[ "boolean", "null", { "type": "array", "items": "double" }]
EOP
    my $enc = '';
    my $data = [ 1, 2, 3, 4, 5, 6 ];
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader        => $reader,
    );

    is_deeply $dec, $data, "decoded!";
}

## map resolution
{
    my $w_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "string" } }
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "int" } }
EOP
    my $enc = '';
    my $data = { "one" => [ "un", "one" ], two => [ "deux", "two" ] };

    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader        => $reader,
        )
    } "Avro::Schema::Error::Mismatch", "recursively... fails";

    open $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $w_schema,
        reader        => $reader,
    );
    is_deeply $dec, $data, "decoded succeeded!";
}

## schema upgrade
{
    my $w_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "int" } }
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "float" } }
EOP
    my $enc = '';
    my $data = { "one" => [ 1, 2 ], two => [ 1, 30 ] };

    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $w_schema,
        reader        => $reader,
    );
    is_deeply $dec, $data, "decoded succeeded! +upgrade";
    is $dec->{one}[0], 1.0, "kind of dumb test";
}

## A bytes/string value declares a length prefix; a malicious or truncated
## input can declare far more bytes than actually exist. On a seekable reader
## that is rejected before allocating for it.
{
    my $bytes_schema = Avro::Schema->parse(q({ "type": "bytes" }));

    ## A length prefix declaring 100 MiB, with no payload following it.
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => Avro::Schema->parse(q({ "type": "long" })),
        data    => 100 * 1024 * 1024,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $bytes_schema,
            reader_schema => $bytes_schema,
            reader        => $reader,
        );
    } qr/Cannot read/, "oversized bytes length is rejected before allocating";

    ## A well-formed value above the check threshold whose data is present
    ## still decodes.
    my $payload = 'x' x (2 * 1024 * 1024);
    my $enc2 = '';
    Avro::BinaryEncoder->encode(
        schema  => $bytes_schema,
        data    => $payload,
        emit_cb => sub { $enc2 .= ${ $_[0] } },
    );
    open my $reader2, '<', \$enc2 or die "Can't open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $bytes_schema,
        reader_schema => $bytes_schema,
        reader        => $reader2,
    );
    is $dec, $payload, "within-limit bytes value still decodes";
}

## An array/map block declares an element count; a malicious or truncated input
## can declare far more elements than the remaining bytes could hold. The count
## is validated against the bytes remaining before iterating, using the minimum
## on-wire size of the element schema (so 0-byte elements like null are not
## falsely rejected).
sub encode_long {
    my ($value) = @_;
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => Avro::Schema->parse(q({ "type": "long" })),
        data    => $value,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    return $enc;
}

{
    my $array_schema = Avro::Schema->parse(q({ "type": "array", "items": "long" }));
    my $enc = encode_long(1_000_000); # 1,000,000 longs, no element data
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $array_schema,
            reader_schema => $array_schema,
            reader        => $reader,
        );
    } qr/Collection claims/, "oversized array count is rejected before iterating";
}

{
    my $map_schema = Avro::Schema->parse(q({ "type": "map", "values": "long" }));
    my $enc = encode_long(1_000_000);
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $map_schema,
            reader_schema => $map_schema,
            reader        => $reader,
        );
    } qr/Collection claims/, "oversized map count is rejected before iterating";
}

{
    ## An array of nulls: null elements occupy zero bytes, so a large count is
    ## legitimate and must not be rejected.
    my $array_schema = Avro::Schema->parse(q({ "type": "array", "items": "null" }));
    my $count = 100_000;
    my $enc = encode_long($count) . encode_long(0); # one block + end marker
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $array_schema,
        reader_schema => $array_schema,
        reader        => $reader,
    );
    is scalar(@$dec), $count, "array of nulls is not falsely rejected";
}

## Zero-byte elements (null, zero-length fixed, all-null records) consume no
## input, so the bytes-remaining check cannot bound them. A huge declared block
## count of such elements is capped instead.
sub decode_zero_byte_array {
    my ($items_schema, $enc) = @_;
    my $schema = Avro::Schema->parse(qq({ "type": "array", "items": $items_schema }));
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    return Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
}

{
    ## The reported exploit: a ~6 byte payload declaring 200,000,000 nulls is
    ## rejected by the default limit, before allocating. Pin the limit to the
    ## default so the test is deterministic even if AVRO_MAX_COLLECTION_ITEMS was
    ## set in the environment when the module was loaded.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS =
        $Avro::BinaryDecoder::DEFAULT_MAX_COLLECTION_ITEMS;
    throws_ok {
        decode_zero_byte_array('"null"', encode_long(200_000_000) . encode_long(0));
    } qr/more than \d+ zero-byte/, "array of 200M nulls rejected by default limit";
}

{
    ## Within a lowered limit, a legitimate small null array still decodes.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    my $dec = decode_zero_byte_array('"null"', encode_long(1000) . encode_long(0));
    is scalar(@$dec), 1000, "array of nulls within configured limit still reads";
}

{
    ## INT64_MIN as a block count is the pathological negation case. Negating it
    ## yields 2**63, which the cap rejects. INT64_MIN zig-zag encodes as the
    ## 10-byte varint below, followed by a block byte-size (0).
    my $int64_min = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01";
    throws_ok {
        decode_zero_byte_array('"null"', $int64_min . encode_long(0));
    } qr/Cannot read a collection|zero-byte|Invalid/, "INT64_MIN array block count rejected";
}

{
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    throws_ok {
        decode_zero_byte_array('"null"', encode_long(1001) . encode_long(0));
    } qr/more than 1000 zero-byte/, "array of nulls over configured limit rejected";
}

{
    ## Cumulative across blocks: two blocks of 600 (1200 > 1000).
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    throws_ok {
        decode_zero_byte_array('"null"', encode_long(600) . encode_long(600) . encode_long(0));
    } qr/more than 1000 zero-byte/, "cumulative null blocks over limit rejected";
}

{
    ## A negative block count (abs value + block byte-size) is normalized and
    ## must still be bounded.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    throws_ok {
        decode_zero_byte_array('"null"', encode_long(-200_000) . encode_long(0));
    } qr/more than 1000 zero-byte/, "negative null block count rejected";
}

{
    ## A record whose only field is null encodes to zero bytes.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    throws_ok {
        decode_zero_byte_array('{ "type": "record", "name": "R", "fields": [{"name":"n","type":"null"}] }',
            encode_long(2000) . encode_long(0));
    } qr/more than 1000 zero-byte/, "array of all-null records over limit rejected";
}

{
    ## A huge map<null> is bounded by the bytes-remaining check (each entry has a
    ## >= 1 byte key), not the zero-byte cap.
    my $map_schema = Avro::Schema->parse(q({ "type": "map", "values": "null" }));
    my $enc = encode_long(200_000_000);
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $map_schema,
            reader_schema => $map_schema,
            reader        => $reader,
        );
    } qr/Collection claims/, "oversized map<null> rejected by available-bytes check";
}

{
    ## Skipping a huge array<null> writer field absent from the reader schema is
    ## bounded (a CPU exhaustion otherwise), via skip_block.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    my $w_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [
            {"name": "arr", "type": {"type": "array", "items": "null"}},
            {"name": "a", "type": "long"} ]}
EOJ
    my $r_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [ {"name": "a", "type": "long"} ]}
EOJ
    # Hand-crafted: array block of 2000 nulls + end marker, then a = 42.
    my $enc = encode_long(2000) . encode_long(0) . encode_long(42);
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader        => $reader,
        );
    } qr/more than 1000/, "skipping oversized array<null> field is bounded";
}

{
    ## The zero-byte cap raises the dedicated exception class.
    local $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS = 1000;
    my $err;
    eval {
        decode_zero_byte_array('"null"', encode_long(2000) . encode_long(0));
        1;
    } or $err = $@;
    isa_ok $err, 'Avro::BinaryDecoder::Error::CollectionSize',
        "zero-byte cap throws CollectionSize";
}

{
    ## Non-zero-byte collections are bounded by a structural cap in addition to
    ## the bytes-remaining check. A small backed array<long> that passes the
    ## bytes check is still rejected once its count exceeds the structural limit.
    local $Avro::BinaryDecoder::MAX_COLLECTION_STRUCTURAL = 5;
    my $schema = Avro::Schema->parse(q({ "type": "array", "items": "long" }));
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $schema,
        data    => [ 0 .. 9 ],   # 10 real longs, enough bytes to pass the byte check
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $schema,
            reader_schema => $schema,
            reader        => $reader,
        );
    } qr/more than 5 elements/, "non-zero collection bounded by structural cap";
}

{
    ## A backed array<long> within the structural cap still decodes.
    local $Avro::BinaryDecoder::MAX_COLLECTION_STRUCTURAL = 100;
    my $schema = Avro::Schema->parse(q({ "type": "array", "items": "long" }));
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $schema,
        data    => [ 0 .. 9 ],
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Can't open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, [ 0 .. 9 ], "backed array within structural cap still reads";
}

## Skipping writer fields absent from the reader schema must advance the reader
## by the correct relative amount (SEEK_CUR), including fixed and bytes fields.
{
    my $w_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [
            {"name": "f", "type": {"type": "fixed", "name": "fix8", "size": 8}},
            {"name": "b", "type": "bytes"},
            {"name": "a", "type": "long"} ]}
EOJ
    my $r_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [ {"name": "a", "type": "long"} ]}
EOJ
    my $data = { f => "01234567", b => "payload", a => 42 };
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader        => $reader,
    );
    is $dec->{a}, 42, "long read correctly after skipping fixed and bytes fields";
    ok ! exists $dec->{f}, "skipped fixed field absent";
    ok ! exists $dec->{b}, "skipped bytes field absent";
}

## Skipping writer array/map fields absent from the reader schema exercises
## skip_block, which is called as a plain function (not a method) by
## skip_array/skip_map.
{
    my $w_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [
            {"name": "arr", "type": {"type": "array", "items": "long"}},
            {"name": "map", "type": {"type": "map", "values": "string"}},
            {"name": "a", "type": "long"} ]}
EOJ
    my $r_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [ {"name": "a", "type": "long"} ]}
EOJ
    my $data = { arr => [ 1, 2, 3 ], map => { x => "y", z => "w" }, a => 99 };
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader        => $reader,
    );
    is $dec->{a}, 99, "long read correctly after skipping array and map fields";
    ok ! exists $dec->{arr}, "skipped array field absent";
    ok ! exists $dec->{map}, "skipped map field absent";
}

## A skipped bytes field whose declared length far exceeds the remaining data
## must be rejected rather than silently seeking past EOF.
{
    my $w_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [
            {"name": "b", "type": "bytes"},
            {"name": "a", "type": "long"} ]}
EOJ
    my $r_schema = Avro::Schema->parse(<<'EOJ');
      { "type": "record", "name": "test",
        "fields" : [ {"name": "a", "type": "long"} ]}
EOJ
    # Declare a bytes length of 100 MiB but provide no data for it.
    my $enc = encode_long(100 * 1024 * 1024);
    open my $reader, '<', \$enc or die "Cannot open memory file: $!";
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader        => $reader,
        );
    } qr/Cannot read/, "oversized skipped bytes length is rejected";
}

done_testing;
