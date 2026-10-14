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

## AVRO-4343: schema-resolution skip path (skip fields present in the writer but
## absent from the reader). Regression tests for the broken skip implementation.
{
    my $enc_with = sub {
        my ($schema, $data) = @_;
        my $out = '';
        Avro::BinaryEncoder->encode(
            schema => $schema, data => $data,
            emit_cb => sub { $out .= ${ $_[0] } },
        );
        return $out;
    };

    ## skip a NON-trailing bytes field, then decode the field after it
    {
        my $w = Avro::Schema->parse(q(
            { "type":"record","name":"S","fields":[
                {"name":"b","type":"bytes"},
                {"name":"a","type":"long"} ]}));
        my $r = Avro::Schema->parse(q(
            { "type":"record","name":"S","fields":[
                {"name":"a","type":"long"} ]}));
        my $enc = $enc_with->($w, { b => "xx", a => 777 });
        open my $reader, '<', \$enc or die $!;
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w, reader_schema => $r, reader => $reader);
        is $dec->{a}, 777, "skip non-trailing bytes field, next field intact";
    }

    ## skip an array field (previously crashed)
    {
        my $w = Avro::Schema->parse(q(
            { "type":"record","name":"A","fields":[
                {"name":"a","type":"long"},
                {"name":"arr","type":{"type":"array","items":"long"}} ]}));
        my $r = Avro::Schema->parse(q(
            { "type":"record","name":"A","fields":[
                {"name":"a","type":"long"} ]}));
        my $enc = $enc_with->($w, { a => 1, arr => [ 1, 2, 3 ] });
        open my $reader, '<', \$enc or die $!;
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w, reader_schema => $r, reader => $reader);
        is $dec->{a}, 1, "skip array field during resolution";
    }

    ## skip a map field (previously crashed)
    {
        my $w = Avro::Schema->parse(q(
            { "type":"record","name":"M","fields":[
                {"name":"a","type":"long"},
                {"name":"m","type":{"type":"map","values":"long"}} ]}));
        my $r = Avro::Schema->parse(q(
            { "type":"record","name":"M","fields":[
                {"name":"a","type":"long"} ]}));
        my $enc = $enc_with->($w, { a => 2, m => { x => 1, y => 2 } });
        open my $reader, '<', \$enc or die $!;
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w, reader_schema => $r, reader => $reader);
        is $dec->{a}, 2, "skip map field during resolution";
    }

    ## skip an array encoded with a NEGATIVE block count (+ block size)
    {
        my $w = Avro::Schema->parse(q(
            { "type":"record","name":"A","fields":[
                {"name":"a","type":"long"},
                {"name":"arr","type":{"type":"array","items":"long"}} ]}));
        my $r = Avro::Schema->parse(q(
            { "type":"record","name":"A","fields":[
                {"name":"a","type":"long"} ]}));
        # a=1; arr: count=-3, size=3, items 1,2,3, end=0
        my $enc = "\x02" . "\x05\x06" . "\x02\x04\x06" . "\x00";
        open my $reader, '<', \$enc or die $!;
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w, reader_schema => $r, reader => $reader);
        is $dec->{a}, 1, "skip array with negative block count";
    }

    ## a negative bytes length in a skipped field must be rejected, not mis-seek
    {
        my $w = Avro::Schema->parse(q(
            { "type":"record","name":"S","fields":[
                {"name":"b","type":"bytes"},
                {"name":"a","type":"long"} ]}));
        my $r = Avro::Schema->parse(q(
            { "type":"record","name":"S","fields":[
                {"name":"a","type":"long"} ]}));
        my $enc = "\x01" . "\x0a";    # b: bytes length -1 (malformed); a=5
        open my $reader, '<', \$enc or die $!;
        throws_ok {
            Avro::BinaryDecoder->decode(
                writer_schema => $w, reader_schema => $r, reader => $reader);
        } qr/negative/, "negative bytes length in skipped field is rejected";
    }
}

done_testing;
