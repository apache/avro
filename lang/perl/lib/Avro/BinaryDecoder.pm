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

package Avro::BinaryDecoder;
use strict;
use warnings;

use Config;
use Encode();
use Error::Simple;
use Avro::Schema;

our $VERSION = '++MODULE_VERSION++';

our $complement = ~0x7F;
unless ($Config{use64bitint}) {
    require Math::BigInt;
    $complement = Math::BigInt->new("0b" . ("1" x 57) . ("0" x 7));
}

=head2 decode(%param)

Resolve the given writer and reader_schema to decode the data provided by the
reader.

=over 4

=item * writer_schema

The schema that was used to encode the data provided by the C<reader>

=item * reader_schema

The schema we want to use to decode the data.

=item * reader

A file handle, or an object implementing a similar interface, like L<IO::File>.
Specifically, it must support C<read($buf, $nbytes)> and
C<seek($nbytes, $whence)>. These calls will block the decoder if not
enough data is available for read.

=back

=cut
sub decode {
    my $class = shift;
    my %param = @_;

    my ($writer_schema, $reader_schema, $reader)
        = @param{qw/writer_schema reader_schema reader/};

    my $type = Avro::Schema->match(
        writer => $writer_schema,
        reader => $reader_schema,
    ) or throw Avro::Schema::Error::Mismatch 'schema do not match';

    my $meth = "decode_$type";
    return $class->$meth($writer_schema, $reader_schema, $reader);
}

sub skip {
    my $class = shift;
    my ($schema, $reader) = @_;
    my $type = ref $schema ? $schema->type : $schema;
    my $meth = "skip_$type";
    return $class->$meth($schema, $reader);
}

sub decode_null { undef }

sub skip_boolean { &decode_boolean }
sub decode_boolean {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $bool, 1);
    return unpack 'C', $bool;
}

sub skip_int { &decode_int }
sub decode_int {
    my $class = shift;
    my $reader = pop;
    return zigzag(unsigned_varint($reader));
}

sub skip_long { &decode_long };
sub decode_long {
    my $class = shift;
    return decode_int($class, @_);
}

sub skip_float { &decode_float }
sub decode_float {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 4);
    return unpack "f<", $buf;
}

sub skip_double { &decode_double }
sub decode_double {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 8);
    return unpack "d<", $buf,
}

sub skip_bytes {
    my $class = shift;
    my $reader = pop;
    my $size = decode_long($class, undef, undef, $reader);
    if ($size < 0) {
        throw Avro::Schema::Error::Parse(
            "Invalid negative bytes/string length: $size");
    }
    # Reject a declared length that exceeds the bytes actually remaining before
    # skipping, mirroring decode_bytes: a seek past EOF silently succeeds on
    # many handles, so without this a truncated input would later read zeros
    # instead of failing.
    _ensure_available($reader, $size, 1);
    # Skip forward by $size bytes relative to the current position (SEEK_CUR);
    # whence 0 (SEEK_SET) would incorrectly seek to the absolute offset $size.
    # A failed seek means the reader cannot skip and is treated as fatal.
    unless ($reader->seek($size, 1)) {
        throw Avro::Schema::Error::Parse("Failed to skip $size bytes");
    }
    return;
}

sub decode_bytes {
    my $class = shift;
    my $reader = pop;
    my $size = decode_long($class, undef, undef, $reader);
    if ($size < 0) {
        throw Avro::Schema::Error::Parse(
            "Invalid negative bytes/string length: $size");
    }
    _ensure_available($reader, $size);
    my $nread = $reader->read(my $buf, $size);
    if (!defined $nread || $nread != $size) {
        # A short read means the input is truncated; failing here avoids
        # returning an under-sized buffer and misaligning the decoder.
        throw Avro::Schema::Error::Parse(
            "Expected $size bytes but read " . (defined $nread ? $nread : 0));
    }
    return $buf;
}

## Reject a declared length that exceeds the bytes actually remaining before
## allocating for it, to guard against an out-of-memory attack from a malicious
## or truncated input. Only enforced for larger reads and when the reader can
## report its size via seek/tell; smaller reads and non-seekable readers fall
## through to a direct read.
use constant _MAX_UNCHECKED_READ => 1024 * 1024;

## Maximum number of zero-byte-encoded collection elements (e.g. an array of
## nulls) to allocate from a single decode. Such elements consume no input, so
## the bytes-remaining check cannot bound their count; without a cap a tiny
## payload can declare a huge block count and exhaust memory. Overridable via the
## AVRO_MAX_COLLECTION_ITEMS environment variable, or by setting
## $Avro::BinaryDecoder::MAX_COLLECTION_ITEMS directly.
our $DEFAULT_MAX_COLLECTION_ITEMS = 10_000_000;
## Structural cap on the number of elements in any array or map (an
## overflow/defense-in-depth guard), matching the historical Integer.MAX_VALUE-8
## limit. Non-zero-byte elements are also bounded by the bytes remaining.
our $DEFAULT_MAX_COLLECTION_STRUCTURAL = (2 ** 31) - 1 - 8;

## AVRO_MAX_COLLECTION_ITEMS, when set, caps both limits; otherwise zero-byte
## elements use the tighter default and all collections use the structural cap.
## Both may also be overridden directly (e.g. in tests).
our $MAX_COLLECTION_ITEMS;
our $MAX_COLLECTION_STRUCTURAL;
if ( defined $ENV{AVRO_MAX_COLLECTION_ITEMS} && $ENV{AVRO_MAX_COLLECTION_ITEMS} =~ /\A[0-9]+\z/ ) {
    $MAX_COLLECTION_ITEMS = $MAX_COLLECTION_STRUCTURAL = $ENV{AVRO_MAX_COLLECTION_ITEMS} + 0;
}
else {
    $MAX_COLLECTION_ITEMS      = $DEFAULT_MAX_COLLECTION_ITEMS;
    $MAX_COLLECTION_STRUCTURAL = $DEFAULT_MAX_COLLECTION_STRUCTURAL;
}

## Number of bytes still available to read, or undef when the reader cannot
## report its size. Used to reject a declared length or collection block count
## that exceeds the data actually available before allocating for it. Readers
## that do not support seeking to the end (e.g. a streaming decompressor) return
## undef, so the check is simply skipped for them.
sub _bytes_remaining {
    my ($reader) = @_;
    my $current = eval { $reader->tell };
    return if !defined $current || $current < 0;

    # Attempt to seek to the end. Readers that cannot (e.g. a streaming
    # decompressor) leave the position unchanged; skip the check for them.
    my $moved = eval { $reader->seek(0, 2) };   # SEEK_END
    return if !$moved;

    # We are now at the end. Record the end offset, then ALWAYS restore the
    # original position. A restore failure leaves the reader corrupted for
    # subsequent decoding, so treat it as fatal rather than continuing.
    my $end = eval { $reader->tell };
    unless (eval { $reader->seek($current, 0) }) {   # SEEK_SET
        throw Avro::Schema::Error::Parse(
            "Failed to restore reader position after size check");
    }
    return if !defined $end || $end < 0;
    return $end - $current;
}

sub _ensure_available {
    my ($reader, $size, $force) = @_;
    # Small reads normally skip the check to avoid per-value overhead, since the
    # decode paths verify the actual read length. The skip paths use seek(),
    # which can silently succeed past EOF and does not verify anything, so they
    # pass $force to always validate against the bytes remaining.
    return if !$force && $size <= _MAX_UNCHECKED_READ;
    my $remaining = _bytes_remaining($reader);
    return unless defined $remaining;
    if ($size > $remaining) {
        throw Avro::Schema::Error::Parse(
            "Cannot read $size bytes, only $remaining remaining");
    }
    return;
}

## Minimum number of bytes a single value of the given schema can occupy on the
## wire. Used to reject an array/map block count that could not be backed by the
## bytes remaining. A type that can encode to zero bytes (null) returns 0, which
## disables the collection check for it (so an array of nulls is not falsely
## rejected).
sub _min_bytes_per_element {
    my ($schema, $visited) = @_;
    $visited ||= {};
    my $type = $schema->type;
    return 0 if $type eq 'null';
    return 4 if $type eq 'float';
    return 8 if $type eq 'double';
    return $schema->size if $type eq 'fixed';
    if ($type eq 'record' || $type eq 'error') {
        my $id = "$schema"; # stringified reference as identity
        return 0 if $visited->{$id};
        $visited->{$id} = 1;
        my $total = 0;
        for my $field (@{ $schema->fields }) {
            $total += _min_bytes_per_element($field->{type}, $visited);
        }
        delete $visited->{$id};
        return $total;
    }
    # boolean, int, long, bytes, string, enum, union, array, map: >= 1 byte
    # (a union encodes at least a 1-byte branch index).
    return 1;
}

## Reject a collection (array or map) block whose declared element count could
## not be backed by the bytes actually remaining, before iterating. Skipped when
## the per-element minimum is zero or when the reader cannot report how many
## bytes remain.
sub _ensure_collection_available {
    my ($reader, $existing, $count, $min_bytes) = @_;
    return if $count <= 0;
    my $total = $existing + $count;
    # The structural cap bounds every array/map, including zero-byte element
    # collections and non-seekable readers where the bytes check cannot run.
    if ($total > $MAX_COLLECTION_STRUCTURAL) {
        throw Avro::BinaryDecoder::Error::CollectionSize(
            "Cannot read a collection of more than $MAX_COLLECTION_STRUCTURAL "
          . "elements (declared $total)");
    }
    if ($min_bytes > 0) {
        my $remaining = _bytes_remaining($reader);
        # Compare via integer division rather than multiplying, so $count * $min_bytes
        # cannot overflow/wrap (notably on 32-bit builds) and defeat the check.
        if (defined $remaining && $count > int($remaining / $min_bytes)) {
            throw Avro::Schema::Error::Parse(
                "Collection claims $count elements with at least $min_bytes bytes each, "
              . "but only $remaining bytes are available");
        }
    }
    # Zero-byte elements (e.g. null) consume no input, so they cannot be bounded
    # by the bytes remaining; additionally cap their cumulative count.
    elsif ($total > $MAX_COLLECTION_ITEMS) {
        throw Avro::BinaryDecoder::Error::CollectionSize(
            "Cannot read a collection of more than $MAX_COLLECTION_ITEMS zero-byte "
          . "elements (declared $total); raise "
          . "\$Avro::BinaryDecoder::MAX_COLLECTION_ITEMS if this is legitimate");
    }
    return;
}

sub skip_string { &skip_bytes }
sub decode_string {
    my $class = shift;
    my $reader = pop;
    my $bytes = decode_bytes($class, undef, undef, $reader);
    return Encode::decode_utf8($bytes);
}

sub skip_record {
    my $class = shift;
    my ($schema, $reader) = @_;
    for my $field (@{ $schema->fields }){
        skip($class, $field->{type}, $reader);
    }
}

## 1.3.2 A record is encoded by encoding the values of its fields in the order
## that they are declared. In other words, a record is encoded as just the
## concatenation of the encodings of its fields. Field values are encoded per
## their schema.
sub decode_record {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $record;

    my %extra_fields = %{ $reader_schema->fields_as_hash };
    for my $field (@{ $writer_schema->fields }) {
        my $name = $field->{name};
        my $w_field_schema = $field->{type};
        my $r_field_schema = delete $extra_fields{$name};

        ## 1.3.2 if the writer's record contains a field with a name not
        ## present in the reader's record, the writer's value for that field
        ## is ignored.
        if (! $r_field_schema) {
            $class->skip($w_field_schema, $reader);
            next;
        }
        my $data = $class->decode(
            writer_schema => $w_field_schema,
            reader_schema => $r_field_schema->{type},
            reader        => $reader,
        );
        $record->{ $name } = $data;
    }

    for my $name (keys %extra_fields) {
        ## 1.3.2. if the reader's record schema has a field with no default
        ## value, and writer's schema does not have a field with the same
        ## name, an error is signalled.
        unless (exists $extra_fields{$name}->{default}) {
            throw Avro::Schema::Error::Mismatch(
                "cannot resolve without default"
            );
        }
        ## 1.3.2 ... else the default value is used
        $record->{ $name } = $extra_fields{$name}->{default};
    }
    return $record;
}

sub skip_enum { &skip_int }

## 1.3.2 An enum is encoded by a int, representing the zero-based position of
## the symbol in the schema.
sub decode_enum {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $index = decode_int($class, @_);

    my $symbols = $writer_schema->symbols;
    ## A negative or out-of-range index is malformed; reject it before indexing
    ## (Perl's negative indexing would otherwise select the wrong symbol).
    if ($index < 0 || $index >= scalar @$symbols) {
        throw Avro::Schema::Error::Parse(
            "Enum symbol index $index out of range for " . scalar(@$symbols) . " symbols");
    }
    my $w_data = $symbols->[$index];
    ## 1.3.2 if the writer's symbol is not present in the reader's enum,
    ## then an error is signalled.
    throw Avro::Schema::Error::Mismatch("enum unknown")
        unless $reader_schema->is_data_valid($w_data);
    return $w_data;
}

sub skip_block {
    # Called as a plain function by skip_array/skip_map as
    # skip_block($reader, $min_bytes, $coderef); it is not a method, so there is
    # no leading class argument. decode_long ignores its (shifted) first argument
    # and reads from the last, so pass __PACKAGE__ for it and $reader last.
    my ($reader, $min_bytes, $block_content) = @_;
    # Zero-byte elements loop with no per-item input, so bound them tightly;
    # other elements consume bytes (bounded by EOF) so only the structural cap
    # applies. Every collection is capped by the structural limit, so for
    # zero-byte elements use whichever of the two limits is tighter.
    my $limit = $MAX_COLLECTION_STRUCTURAL;
    if ($min_bytes <= 0 && $MAX_COLLECTION_ITEMS < $limit) {
        $limit = $MAX_COLLECTION_ITEMS;
    }
    my $block_count = decode_long(__PACKAGE__, undef, undef, $reader);
    my $skipped = 0;
    while ($block_count) {
        if ($block_count < 0) {
            # A byte-sized (negative-count) block still declares an element
            # count; bound it cumulatively too, so a huge declared count with a
            # small or zero block size cannot bypass the cap (e.g. zero-byte
            # elements whose block size is 0).
            my $count = -$block_count;
            if ($skipped + $count > $limit) {
                throw Avro::BinaryDecoder::Error::CollectionSize(
                    "Cannot skip a collection of more than $limit "
                  . "elements (declared @{[ $skipped + $count ]})");
            }
            $skipped += $count;
            # A negative count is followed by a long block size in bytes, which
            # lets the whole block be skipped without decoding each item. Skip
            # forward by that many bytes relative to the current position
            # (SEEK_CUR); whence 0 (SEEK_SET) would seek to an absolute (here
            # nonsensical) offset. A failed seek is treated as fatal.
            my $block_size = decode_long(__PACKAGE__, undef, undef, $reader);
            if ($block_size < 0) {
                # A negative block size would seek the reader backwards,
                # risking an infinite loop or mis-decoding of a corrupt input.
                throw Avro::Schema::Error::Parse(
                    "Invalid negative block size: $block_size");
            }
            # Reject a block size that exceeds the bytes actually remaining
            # before skipping, so a truncated input fails instead of seeking
            # past EOF.
            _ensure_available($reader, $block_size, 1);
            unless ($reader->seek($block_size, 1)) {
                throw Avro::Schema::Error::Parse(
                    "Failed to skip block of $block_size bytes");
            }
        }
        else {
            # Bound the cumulative element count: skipping a huge block of
            # zero-byte elements would otherwise loop unboundedly (a CPU
            # exhaustion) even though it allocates nothing.
            if ($skipped + $block_count > $limit) {
                throw Avro::BinaryDecoder::Error::CollectionSize(
                    "Cannot skip a collection of more than $limit "
                  . "elements (declared @{[ $skipped + $block_count ]})");
            }
            $skipped += $block_count;
            for (1..$block_count) {
                $block_content->();
            }
        }
        $block_count = decode_long(__PACKAGE__, undef, undef, $reader);
    }
}

sub skip_array {
    my $class = shift;
    my ($schema, $reader) = @_;
    skip_block($reader, _min_bytes_per_element($schema->items),
        sub { $class->skip($schema->items, $reader) });
}

## 1.3.2 Arrays are encoded as a series of blocks. Each block consists of a
## long count value, followed by that many array items. A block with count zero
## indicates the end of the array. Each item is encoded per the array's item
## schema.
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size
sub decode_array {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $block_count = decode_long($class, @_);
    my @array;
    my $writer_items = $writer_schema->items;
    my $reader_items = $reader_schema->items;
    my $min_bytes = _min_bytes_per_element($writer_items);
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            if ($block_size < 0) {
                throw Avro::Schema::Error::Parse(
                    "Invalid negative array block size: $block_size");
            }
            ## XXX we can skip with $reader_schema?
        }
        _ensure_collection_available($reader, scalar(@array), $block_count, $min_bytes);
        for (1..$block_count) {
            push @array, $class->decode(
                writer_schema => $writer_items,
                reader_schema => $reader_items,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \@array;
}

sub skip_map {
    my $class = shift;
    my ($schema, $reader) = @_;
    # Map entries always carry a >= 1 byte key, so pass a positive minimum.
    skip_block($reader, 1 + _min_bytes_per_element($schema->values), sub {
        skip_string($class, $reader);
        $class->skip($schema->values, $reader);
    });
}

## 1.3.2 Maps are encoded as a series of blocks. Each block consists of a long
## count value, followed by that many key/value pairs. A block with count zero
## indicates the end of the map. Each item is encoded per the map's value
## schema.
##
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size indicating the number of bytes in
## the block. This block size permits fast skipping through data, e.g., when
## projecting a record to a subset of its fields.
sub decode_map {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my %hash;

    my $block_count = decode_long($class, @_);
    my $writer_values = $writer_schema->values;
    my $reader_values = $reader_schema->values;
    # A map key is a non-empty string (decode_map rejects empty keys below), so
    # its minimum on-wire size is 2 bytes: a 1-byte length prefix plus at least
    # 1 byte of key data, in addition to the value.
    my $min_bytes = 2 + _min_bytes_per_element($writer_values);
    # Track the number of decoded entries separately: scalar(keys %hash)
    # undercounts when duplicate keys overwrite earlier entries, which would let
    # a multi-block map exceed the cumulative caps without being rejected.
    my $decoded = 0;
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            if ($block_size < 0) {
                throw Avro::Schema::Error::Parse(
                    "Invalid negative map block size: $block_size");
            }
            ## XXX we can skip with $reader_schema?
        }
        _ensure_collection_available($reader, $decoded, $block_count, $min_bytes);
        $decoded += $block_count;
        for (1..$block_count) {
            my $key = decode_string($class, @_);
            unless (defined $key && length $key) {
                throw Avro::Schema::Error::Parse("key of map is invalid");
            }
            $hash{$key} = $class->decode(
                writer_schema => $writer_values,
                reader_schema => $reader_values,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \%hash;
}

sub skip_union {
    my $class = shift;
    my ($schema, $reader) = @_;
    my $idx = decode_long($class, undef, undef, $reader);
    my $schemas = $schema->schemas;
    if ($idx < 0 || $idx >= scalar @$schemas) {
        throw Avro::Schema::Error::Parse("union union member");
    }
    my $union_schema = $schemas->[$idx];
    $class->skip($union_schema, $reader);
}

## 1.3.2 A union is encoded by first writing an int value indicating the
## zero-based position within the union of the schema of its value. The value
## is then encoded per the indicated schema within the union.
sub decode_union {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $idx = decode_long($class, @_);
    my $schemas = $writer_schema->schemas;
    ## A negative or out-of-range branch index is malformed; reject it before
    ## indexing (skip_union performs the same check).
    if ($idx < 0 || $idx >= scalar @$schemas) {
        throw Avro::Schema::Error::Parse(
            "Union branch index $idx out of range for " . scalar(@$schemas) . " branches");
    }
    my $union_schema = $schemas->[$idx];
    ## XXX TODO: schema resolution
    # The first schema in the reader's union that matches the selected writer's
    # union schema is recursively resolved against it. if none match, an error
    # is signalled.
    return $class->decode(
        reader_schema => $union_schema,
        writer_schema => $union_schema,
        reader => $reader,
    );
}

sub skip_fixed {
    my $class = shift;
    my ($schema, $reader) = @_;
    # Reject a fixed size that exceeds the bytes actually remaining before
    # skipping, mirroring skip_bytes: a seek past EOF silently succeeds on many
    # handles, so without this a truncated input would later read zeros instead
    # of failing.
    _ensure_available($reader, $schema->size, 1);
    # Skip the fixed-size payload relative to the current position (SEEK_CUR);
    # whence 0 (SEEK_SET) would incorrectly seek to the absolute offset. A
    # failed seek is treated as fatal.
    unless ($reader->seek($schema->size, 1)) {
        throw Avro::Schema::Error::Parse(
            "Failed to skip " . $schema->size . " bytes");
    }
}

## 1.3.2 Fixed instances are encoded using the number of bytes declared in the
## schema.
sub decode_fixed {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    $reader->read(my $buf, $writer_schema->size);
    return $buf;
}

sub zigzag {
    my $int = shift;
    if (1 & $int) {
        ## odd values are encoded negative ints
        return -( 1 + ($int >> 1) );
    }
    ## even values are positive natural left shifted one bit
    else {
        return $int >> 1;
    }
}

sub unsigned_varint {
    my $reader = shift;
    my $int = 0;
    my $more;
    my $shift = 0;
    do {
        # A 64-bit value uses at most 10 bytes (shifts 0..63); reject an overlong
        # varint rather than reading an unbounded continuation chain and letting
        # $shift overflow into a garbage value.
        if ($shift >= 70) {
            throw Avro::Schema::Error::Parse("Varint is too long");
        }
        my $got = $reader->read(my $buf, 1);
        # A short read (EOF) would otherwise make `ord $buf` == 0 and silently
        # decode truncated input as a valid 0 byte; treat it as a parse error.
        if (!$got) {
            throw Avro::Schema::Error::Parse("Unexpected end of input while reading varint");
        }
        my $byte = ord $buf;
        my $value = $byte & 0x7F;
        $int |= $value << $shift;
        $shift += 7;
        $more = $byte & 0x80;
    } until (! $more);
    return $int;
}

package Avro::BinaryDecoder::Error::CollectionSize;
use parent -norequire, 'Error::Simple';

package Avro::BinaryDecoder;

1;
