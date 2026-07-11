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

package Avro::DataFileReader;
use strict;
use warnings;

use Object::Tiny qw{
    fh
    reader_schema
    sync_marker
    block_max_size
};

use constant MARKER_SIZE => 16;

# A data-file block is decompressed according to the file's codec. A block with
# a very high compression ratio (or a malformed block) can expand to far more
# memory than its compressed size. To guard against unbounded allocation, the
# decompressed size of a single block is capped. This mirrors the Java SDK's
# decompression limit (AVRO-4247). The default can be overridden with the
# AVRO_MAX_DECOMPRESS_LENGTH environment variable.
use constant DEFAULT_MAX_DECOMPRESS_LENGTH => 200 * 1024 * 1024; # 200 MiB

use Avro::DataFile;
use Avro::BinaryDecoder;
use Avro::Schema;
use Carp;
use IO::Uncompress::Bunzip2 ();
use IO::Uncompress::RawInflate ;
use Fcntl();
use bytes ();

our $VERSION = '++MODULE_VERSION++';

sub new {
    my $class = shift;
    my $datafile = $class->SUPER::new(@_);

    my $schema = $datafile->{reader_schema};
    croak "schema is invalid"
        if $schema && ! eval { $schema->isa("Avro::Schema") };

    return $datafile;
}

sub codec {
    my $datafile = shift;
    return $datafile->metadata->{'avro.codec'} || 'null';
}

sub writer_schema {
    my $datafile = shift;
    unless (exists $datafile->{_writer_schema}) {
        my $json_schema = $datafile->metadata->{'avro.schema'};
        $datafile->{_writer_schema} = Avro::Schema->parse($json_schema);
    }
    return $datafile->{_writer_schema};
}

sub metadata {
    my $datafile = shift;
    unless (exists $datafile->{_metadata}) {
        my $header = $datafile->header;
        $datafile->{_metadata} = $header->{meta} || {};
    }
    return $datafile->{_metadata};
}

sub header {
    my $datafile = shift;
    unless (exists $datafile->{_header}) {
        $datafile->{_header} = $datafile->read_file_header;
    }

    return $datafile->{_header};
}

sub read_file_header {
    my $datafile = shift;

    my $data = Avro::BinaryDecoder->decode(
        reader_schema => $Avro::DataFile::HEADER_SCHEMA,
        writer_schema => $Avro::DataFile::HEADER_SCHEMA,
        reader        => $datafile->{fh},
    );
    croak "Magic '$data->{magic}' doesn't match"
        unless $data->{magic} eq Avro::DataFile->AVRO_MAGIC;

    $datafile->{sync_marker} = $data->{sync}
        or croak "sync marker appears invalid";

    my $codec = $data->{meta}{'avro.codec'} || 'null';

    throw Avro::DataFile::Error::UnsupportedCodec($codec)
        unless Avro::DataFile->is_codec_valid($codec);

    return $data;
}

sub all {
    my $datafile = shift;

    my @objs;
    my @block_objs;
    do {
        if ($datafile->eof) {
            @block_objs = ();
        }
        else {
            $datafile->read_block_header if $datafile->eob;
            @block_objs = $datafile->read_to_block_end;
            push @objs, @block_objs;
        }

    } until !@block_objs;

    return @objs
}

sub next {
    my $datafile = shift;
    my $count    = shift;

    my @objs;

    return ()                    if $datafile->eof;
    $datafile->read_block_header if $datafile->eob;

    my $block_count = $datafile->{object_count};

    if ($block_count < $count) {
        push @objs, $datafile->read_to_block_end;
        croak "Didn't read as many objects than expected"
            unless scalar @objs == $block_count;

        push @objs, $datafile->next($count - $block_count);
    }
    else {
        push @objs, $datafile->read_within_block($count);
    }
    return @objs;
}

sub read_within_block {
    my $datafile = shift;
    my $count    = shift;

    my $reader        = $datafile->reader;
    my $writer_schema = $datafile->writer_schema;
    my $reader_schema = $datafile->reader_schema || $writer_schema;
    my @objs;
    while ($count-- > 0 && $datafile->{object_count} > 0) {
        push @objs, Avro::BinaryDecoder->decode(
            writer_schema => $writer_schema,
            reader_schema => $reader_schema,
            reader        => $reader,
        );
        $datafile->{object_count}--;
    }
    return @objs;
}

sub skip {
    my $datafile = shift;
    my $count    = shift;

    my $block_count = $datafile->{object_count};
    if ($block_count <= $count) {
        $datafile->skip_to_block_end
            or croak "Cannot skip to end of block!";
        $datafile->skip($count - $block_count);
    }
    else {
        my $writer_schema = $datafile->writer_schema;
        ## could probably be optimized
        while ($count--) {
            Avro::BinaryDecoder->skip($writer_schema, $datafile->reader);
            $datafile->{object_count}--;
        }
    }
}

sub read_block_header {
    my $datafile = shift;
    my $fh = $datafile->{fh};
    my $codec = $datafile->codec;

    $datafile->header unless $datafile->{_header};

    $datafile->{object_count} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_size} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_start} = tell $fh;

    return if $codec eq 'null';

    ## Guard against an attacker-controlled block_size triggering a huge
    ## allocation for the compressed block itself, before any decompression
    ## happens. When the reader is configured with block_max_size, reject a
    ## block whose declared compressed size exceeds that bound up front.
    my $block_max = $datafile->{block_max_size};
    if (defined $block_max && $datafile->{block_size} > $block_max) {
        Avro::DataFile::Error::CompressedBlockSize->throw(
            "Compressed block size $datafile->{block_size} exceeds the configured block_max_size of $block_max bytes"
        );
    }

    ## we need to read the entire block into memory, to inflate it
    my $nread = read $fh, my $block, $datafile->{block_size} + MARKER_SIZE
        or croak "Error reading from file: $!";

    ## remove the marker
    my $marker = substr $block, -(MARKER_SIZE), MARKER_SIZE, '';
    $datafile->{block_marker} = $marker;

    ## The decompressed size of a block is capped to guard against a block with
    ## a very high compression ratio expanding to far more memory than its
    ## compressed size. The limit is the AVRO_MAX_DECOMPRESS_LENGTH environment
    ## variable, or DEFAULT_MAX_DECOMPRESS_LENGTH. (Note: block_max_size is a
    ## writer-side flush threshold measured in compressed bytes and is not reused
    ## here to avoid conflating the two units.)
    my $limit = _max_decompress_length();

    ## this is our new reader
    $datafile->{reader} = do {
        if ($codec eq 'deflate') {
            my $z = IO::Uncompress::RawInflate->new(\$block)
                or croak "Error inflating block: $IO::Uncompress::RawInflate::RawInflateError";
            my $uncompressed = _inflate_bounded($z, $limit);
            _open_decompressed(\$uncompressed);
        }
        elsif ($codec eq 'bzip2') {
            my $z = IO::Uncompress::Bunzip2->new(\$block)
                or croak "Error decompressing bzip2 block: $IO::Uncompress::Bunzip2::Bunzip2Error";
            my $uncompressed = _inflate_bounded($z, $limit);
            _open_decompressed(\$uncompressed);
        }
        elsif ($codec eq 'zstandard') {
            my $uncompressed = _zstd_decompress_bounded(\$block, $limit);
            _open_decompressed(\$uncompressed);
        }
    };

    return;
}

## Open an in-memory read handle over the decompressed block, surfacing any
## failure via croak rather than leaving $datafile->{reader} undefined (which
## would fail later with a less clear error). The handle keeps a reference to
## the scalar, so the caller's buffer stays alive for the lifetime of the read.
sub _open_decompressed {
    my ($uncompressed_ref) = @_;
    open my $fh, '<', $uncompressed_ref
        or croak "Error opening decompressed block for reading: $!";
    return $fh;
}

## Read from a streaming decompressor in chunks, rejecting the block as soon as
## its decompressed size would exceed $limit so an over-large (or malicious)
## block is not fully materialized in memory. Each read is sized to the
## remaining budget (capped at 64 KiB) so the buffer overshoots $limit by at
## most one byte before the check fires.
sub _inflate_bounded {
    my ($z, $limit) = @_;
    my $uncompressed = '';
    my $chunk;
    my $status;
    while (1) {
        my $budget = $limit - bytes::length($uncompressed) + 1;
        my $to_read = $budget < 65536 ? $budget : 65536;
        $status = $z->read($chunk, $to_read);
        last unless defined $status && $status > 0;
        $uncompressed .= $chunk;
        _check_decompress_length(bytes::length($uncompressed), $limit);
    }
    if (!defined $status || $status < 0) {
        croak "Error decompressing block: " . $z->error;
    }
    return $uncompressed;
}

## Streaming zstandard decompression, bounded the same way as _inflate_bounded so
## a high-ratio block is rejected before its full form is materialized.
sub _zstd_decompress_bounded {
    my ($block_ref, $limit) = @_;
    # Load the zstandard decompressor lazily so the reader still loads and works
    # for other codecs when Compress::Zstd::Decompressor is unavailable (e.g. an
    # older Compress::Zstd distribution that lacks the Decompressor submodule).
    unless (eval { require Compress::Zstd::Decompressor; 1 }) {
        Avro::DataFile::Error::UnsupportedCodec->throw(
            "Cannot read zstandard-compressed block: Compress::Zstd::Decompressor is not available"
        );
    }
    my $decompressor = Compress::Zstd::Decompressor->new;
    my $uncompressed = '';
    my $length = bytes::length($$block_ref);
    my $offset = 0;
    while ($offset < $length) {
        my $piece = substr($$block_ref, $offset, 65536);
        $offset += 65536;
        my $out = $decompressor->decompress($piece);
        # The streaming decompressor croaks on a corrupt frame and otherwise
        # emits all output produced while consuming the input it is given (there
        # is no separate flush step in this API). Treat an undefined return as a
        # failure and fail closed, rather than silently skipping it, so a
        # malformed block cannot masquerade as a short, within-limit result.
        unless (defined $out) {
            croak "Error decompressing zstandard block";
        }
        # Check the prospective total before growing $uncompressed so a single
        # large decompressed chunk cannot transiently balloon memory past the
        # limit (or double peak memory from string reallocation).
        _check_decompress_length(
            bytes::length($uncompressed) + bytes::length($out), $limit);
        $uncompressed .= $out;
    }
    return $uncompressed;
}

sub _check_decompress_length {
    my ($length, $limit) = @_;
    if ($length > $limit) {
        Avro::DataFile::Error::DecompressionSize->throw(
            "Decompressed block size exceeds the maximum allowed of $limit bytes"
        );
    }
    return;
}

sub _max_decompress_length {
    my $value = $ENV{AVRO_MAX_DECOMPRESS_LENGTH};
    if (defined $value && $value =~ /\A[0-9]+\z/ && $value > 0) {
        return $value + 0;
    }
    return DEFAULT_MAX_DECOMPRESS_LENGTH;
}

sub verify_marker {
    my $datafile = shift;

    my $marker = $datafile->{block_marker};
    unless (defined $marker) {
        ## we are in the fh case
        read $datafile->{fh}, $marker, MARKER_SIZE;
    }

    unless (($marker || "") eq $datafile->sync_marker) {
        croak "Oops synchronization issue (marker mismatch)";
    }
    return;
}

sub skip_to_block_end {
    my $datafile = shift;

    if (my $reader = $datafile->{reader}) {
        seek $reader, 0, Fcntl->SEEK_END;
        return;
    }

    my $remaining_size = $datafile->{block_size}
                       + $datafile->{block_start}
                       - tell $datafile->{fh};

    seek $datafile->{fh}, $remaining_size, 0;
    $datafile->verify_marker; ## will do a read
    return 1;
}

sub read_to_block_end {
    my $datafile = shift;

    my $reader = $datafile->reader;
    my @objs = $datafile->read_within_block( $datafile->{object_count} );
    $datafile->verify_marker;
    return @objs;
}

sub reader {
    my $datafile = shift;
    return $datafile->{reader} || $datafile->{fh};
}

## end of block
sub eob {
    my $datafile = shift;

    return 1 if $datafile->eof;

    if ($datafile->{reader}) {
        return 1 if $datafile->{reader}->eof;
    }
    else {
        my $pos = tell $datafile->{fh};
        return 1 unless $datafile->{block_start};
        return 1 if $pos >= $datafile->{block_start} + $datafile->{block_size};
    }
    return 0;
}

sub eof {
    my $datafile = shift;
    if ($datafile->{reader}) {
        return 0 unless $datafile->{reader}->eof;
    }
    return 1 if $datafile->{fh}->eof;
    return 0;
}

package Avro::DataFile::Error::UnsupportedCodec;
use parent 'Error::Simple';

package Avro::DataFile::Error::DecompressionSize;
use parent 'Error::Simple';

## Raised when a block's declared *compressed* size exceeds the reader's
## configured block_max_size, before the block is read into memory. Kept
## distinct from DecompressionSize (which is about the *decompressed* size cap)
## so callers can tell the two conditions apart.
package Avro::DataFile::Error::CompressedBlockSize;
use parent 'Error::Simple';

1;
