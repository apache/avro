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

package Avro::BinaryEncoder;
use strict;
use warnings;

use Config;
use Encode();
use Error::Simple;
use Regexp::Common qw(number);
use JSON::PP; # For is_bool

our $VERSION = '++MODULE_VERSION++';

# Private function deleted below, should be a lexical sub
sub _bigint {
    require Math::BigInt;
    my $val = Math::BigInt->new(shift);

    $Config{use64bitint}
        ? 0 + $val->bstr() # numify() loses precision
        : $val;
}

# Avro type limits
# Private constants deleted below
use constant INT_MAX => 0x7FFF_FFFF;
use constant INT_MIN => -0x8000_0000;
use constant LONG_MAX => _bigint('0x7FFF_FFFF_FFFF_FFFF');
use constant LONG_MIN => _bigint('-0x8000_0000_0000_0000');

=head2 encode(%param)

Encodes the given C<data> according to the given C<schema>, and pass it
to the C<emit_cb>

Params are:

=over 4

=item * data

The data to encode (can be any perl data structure, but it should match
schema)

=item * schema

The schema to use to encode C<data>

=item * emit_cb($byte_ref)

The callback that will be invoked with the a reference to the encoded data
in parameters.

=back

=cut

sub encode {
    my $class = shift;
    my %param = @_;
    my ($schema, $data, $cb) = @param{qw/schema data emit_cb/};

    ## a schema can also be just a string
    my $type = ref $schema ? $schema->type : $schema;

    ## might want to profile and optimize this
    my $meth = "encode_$type";
    $class->$meth($schema, $data, $cb);
    return;
}

sub encode_null {
    $_[3]->(\'');
}

sub encode_boolean {
    my $class = shift;
    my ($schema, $data, $cb) = @_;

    throw Avro::BinaryEncoder::Error( "<UNDEF> is not a valid boolean value")
        unless defined $data;

    if ( my $type = ref $data ) {
        throw Avro::BinaryEncoder::Error("cannot encode a '$type' reference as boolean")
            unless $type eq 'JSON::PP::Boolean';
    }
    else {
        throw Avro::BinaryEncoder::Error( "'$data' is not a valid boolean value")
            unless $data eq '' # For Perl versions without builtin::is_bool
                || JSON::PP::is_bool($data)
                || $data =~ /^(?:true|t|false|f|yes|y|no|n|0|1)$/i;
    }

    # Some values might be false but evaluate to "truthy" for Perl,
    # like the string 'false'. For these known exceptions, which
    # would otherwise be false, we read a false value. For everything
    # else, we rely on their value evaluated in a boolean context.
    my $true = $data =~ /^(?:no|n|false|f)$/i ? 0 : !!$data;

    $cb->( $true ? \"\x1" : \"\x0" );
}

sub encode_int {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    if ($data !~ /^$RE{num}{int}$/) {
        throw Avro::BinaryEncoder::Error("cannot convert '$data' to integer");
    }
    if ($data > INT_MAX || $data < INT_MIN) {
        throw Avro::BinaryEncoder::Error("data ($data) out of range for Avro 'int'");
    }

    my $enc = unsigned_varint(zigzag($data));
    $cb->(\$enc);
}

sub encode_long {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    if ($data !~ /^$RE{num}{int}$/) {
        throw Avro::BinaryEncoder::Error("cannot convert '$data' to long integer");
    }
    if ($data > LONG_MAX || $data < LONG_MIN) {
        throw Avro::BinaryEncoder::Error("data ($data) out of range for Avro 'long'");
    }
    my $enc = unsigned_varint(zigzag($data));
    $cb->(\$enc);
}

sub encode_float {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $enc = pack "f<", $data;
    $cb->(\$enc);
}

sub encode_double {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $enc = pack "d<", $data;
    $cb->(\$enc);
}

sub encode_bytes {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    throw Avro::BinaryEncoder::Error("Invalid data given for 'bytes': Contains values >255")
        unless utf8::downgrade($data, 1);
    encode_long($class, undef, length($data), $cb);
    $cb->(\$data);
}

sub encode_string {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $bytes = Encode::encode_utf8($data);
    encode_long($class, undef, length($bytes), $cb);
    $cb->(\$bytes);
}

## 1.3.2 A record is encoded by encoding the values of its fields in the order
## that they are declared. In other words, a record is encoded as just the
## concatenation of the encodings of its fields. Field values are encoded per
## their schema.
sub encode_record {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    for my $field (@{ $schema->fields }) {
        $class->encode(
            schema  => $field->{type},
            data    => $data->{ $field->{name} },
            emit_cb => $cb,
        );
    }
}

## 1.3.2 An enum is encoded by a int, representing the zero-based position of
## the symbol in the schema.
sub encode_enum {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $symbols = $schema->symbols_as_hash;
    my $pos = $symbols->{ $data };
    throw Avro::BinaryEncoder::Error("Cannot find enum $data")
        unless defined $pos;
    $class->encode_int(undef, $pos, $cb);
}

## 1.3.2 Arrays are encoded as a series of blocks. Each block consists of a
## long count value, followed by that many array items. A block with count zero
## indicates the end of the array. Each item is encoded per the array's item
## schema.
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size

## maybe here it would be worth configuring what a typical block size should be
sub encode_array {
    my $class = shift;
    my ($schema, $data, $cb) = @_;

    ## FIXME: multiple blocks
    if (@$data) {
        $class->encode_long(undef, scalar @$data, $cb);
        for (@$data) {
            $class->encode(
                schema => $schema->items,
                data => $_,
                emit_cb => $cb,
            );
        }
    }
    ## end of the only block
    $class->encode_long(undef, 0, $cb);
}


## 1.3.2 Maps are encoded as a series of blocks. Each block consists of a long
## count value, followed by that many key/value pairs. A block with count zero
## indicates the end of the map. Each item is encoded per the map's value
## schema.
##
## (TODO)
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size indicating the number of bytes in
## the block. This block size permits fast skipping through data, e.g., when
## projecting a record to a subset of its fields.
sub encode_map {
    my $class = shift;
    my ($schema, $data, $cb) = @_;

    my @keys = keys %$data;
    if (@keys) {
        $class->encode_long(undef, scalar @keys, $cb);
        for (@keys) {
            ## the key
            $class->encode_string(undef, $_, $cb);

            ## the value
            $class->encode(
                schema => $schema->values,
                data => $data->{$_},
                emit_cb => $cb,
            );
        }
    }
    ## end of the only block
    $class->encode_long(undef, 0, $cb);
}

## 1.3.2 A union is encoded by first writing an int value indicating the
## zero-based position within the union of the schema of its value. The value
## is then encoded per the indicated schema within the union.
sub encode_union {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $idx = 0;
    my $elected_schema;
    for my $inner_schema (@{$schema->schemas}) {
        if ($inner_schema->is_data_valid($data)) {
            $elected_schema = $inner_schema;
            last;
        }
        $idx++;
    }
    unless ($elected_schema) {
        throw Avro::BinaryEncoder::Error("union cannot validate the data");
    }
    $class->encode_long(undef, $idx, $cb);
    $class->encode(
        schema => $elected_schema,
        data => $data,
        emit_cb => $cb,
    );
}

## 1.3.2 Fixed instances are encoded using the number of bytes declared in the
## schema.
sub encode_fixed {
    my $class = shift;
    my ($schema, $data, $cb) = @_;

    throw Avro::BinaryEncoder::Error("Invalid data given for 'fixed': Contains values >255")
        unless utf8::downgrade($data, 1);

    my $length = length $data;
    my $size   = $schema->size;

    throw Avro::BinaryEncoder::Error("Fixed size doesn't match $length!=$size")
        unless $length == $size;

    $cb->(\$data);
}

sub zigzag {
    use warnings FATAL => 'numeric';
    if ( $_[0] >= 0 ) {
        return $_[0] << 1;
    }
    return (($_[0] << 1) ^ -1) | 0x1;
}

sub unsigned_varint {
    my @bytes;
    while ($_[0] > 0x7F) {                  # while more than 7 bits to encode
        push @bytes, ($_[0] & 0x7F) | 0x80; # append continuation bit and 7 data bits
        $_[0] >>= 7;                        # get next 7 bits
    }
    push @bytes, $_[0]; # last byte
    return pack "C*", @bytes;
}

# Delete private symbols to avoid adding them to the API
delete $Avro::BinaryEncoder::{$_} for '_bigint', <{INT,LONG}_{MIN,MAX}>;

package Avro::BinaryEncoder::Error;
use parent 'Error::Simple';

1;
