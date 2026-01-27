<?php

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro\DataFile;

use Apache\Avro\AvroException;
use Apache\Avro\AvroIO;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\IO\AvroIOException;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

/**
 * Reads Avro data from an AvroIO source using an AvroSchema.
 */
class AvroDataIOReader
{
    public string $sync_marker;
    /**
     * @var array<string, mixed> object container metadata
     */
    public array $metadata;

    private AvroIOBinaryDecoder $decoder;
    /**
     * @var int count of items in block
     */
    private int $blockCount;

    /**
     * @var string compression codec
     */
    private string $codec;

    /**
     * @param AvroIO $io source from which to read
     * @param AvroIODatumReader $datumReader reader that understands
     *                                        the data schema
     * @throws AvroDataIOException if $io is not an instance of AvroIO
     *                             or the codec specified in the header
     *                             is not supported
     * @uses readHeader()
     */
    public function __construct(
        private AvroIO $io,
        private AvroIODatumReader $datumReader
    ) {
        $this->decoder = new AvroIOBinaryDecoder($this->io);
        $this->readHeader();

        $codec = $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] ?? null;
        if ($codec && !AvroDataIO::isValidCodec($codec)) {
            throw new AvroDataIOException(sprintf('Unknown codec: %s', $codec));
        }
        $this->codec = $codec;

        $this->blockCount = 0;
        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datumReader->setWritersSchema(
            AvroSchema::parse($this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR])
        );
    }

    /**
     * @throws AvroException
     * @throws AvroIOException
     * @return list<mixed> of data from object container.
     * @internal Would be nice to implement data() as an iterator, I think
     */
    public function data(): array
    {
        $data = [];
        $decoder = $this->decoder;
        while (true) {
            if (0 == $this->blockCount) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync()) {
                    /** @phpstan-ignore if.alwaysFalse */
                    if ($this->isEof()) {
                        break;
                    }
                }

                $length = $this->readBlockHeader();
                if (AvroDataIO::DEFLATE_CODEC === $this->codec) {
                    $compressed = $decoder->read($length);
                    $datum = $this->gzUncompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                } elseif (AvroDataIO::ZSTANDARD_CODEC === $this->codec) {
                    $compressed = $decoder->read($length);
                    $datum = $this->zstdUncompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                } elseif (AvroDataIO::SNAPPY_CODEC === $this->codec) {
                    $compressed = $decoder->read($length);
                    $datum = $this->snappyUncompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                } elseif (AvroDataIO::BZIP2_CODEC === $this->codec) {
                    $compressed = $decoder->read($length);
                    $datum = $this->bzUncompress($compressed);
                    $decoder = new AvroIOBinaryDecoder(new AvroStringIO($datum));
                }
            }
            $data[] = $this->datumReader->read($decoder);
            --$this->blockCount;
        }

        return $data;
    }

    /**
     * Closes this writer (and its AvroIO object.)
     * @uses AvroIO::close()
     */
    public function close(): bool
    {
        return $this->io->close();
    }

    /**
     * Reads header of object container
     * @throws AvroDataIOException if the file is not an Avro data file.
     */
    private function readHeader(): void
    {
        $this->seek(0, AvroIO::SEEK_SET);

        $magic = $this->read(AvroDataIO::magicSize());

        if (strlen((string) $magic) < AvroDataIO::magicSize()) {
            throw new AvroDataIOException(
                'Not an Avro data file: shorter than the Avro magic block'
            );
        }

        if (AvroDataIO::magic() != $magic) {
            throw new AvroDataIOException(
                sprintf(
                    'Not an Avro data file: %s does not match %s',
                    $magic,
                    AvroDataIO::magic()
                )
            );
        }

        $this->metadata = $this->datumReader->readData(
            AvroDataIO::metadataSchema(),
            AvroDataIO::metadataSchema(),
            $this->decoder
        );
        $this->sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
    }

    /**
     * @uses AvroIO::seek()
     */
    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @uses AvroIO::read()
     */
    private function read(int $len): string
    {
        return $this->io->read($len);
    }

    /**
     * @uses AvroIO::isEof()
     */
    private function isEof(): bool
    {
        return $this->io->isEof();
    }

    private function skipSync(): bool
    {
        $proposedSyncMarker = $this->read(AvroDataIO::SYNC_SIZE);
        if ($proposedSyncMarker !== $this->sync_marker) {
            $this->seek(-AvroDataIO::SYNC_SIZE, AvroIO::SEEK_CUR);

            return false;
        }

        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block
     * and the length in bytes of the block)
     * @return int|string length in bytes of the block. It returns a string if AvroGMP is enabled.
     */
    private function readBlockHeader(): string|int
    {
        $this->blockCount = $this->decoder->readLong();

        return $this->decoder->readLong();
    }

    /**
     * @throws AvroException
     */
    private function gzUncompress(string $compressed): string
    {
        $datum = gzinflate($compressed);

        if (false === $datum) {
            throw new AvroException('gzip uncompression failed.');
        }

        return $datum;
    }

    /**
     * @throws AvroException
     */
    private function zstdUncompress(string $compressed): string
    {
        if (!extension_loaded('zstd')) {
            throw new AvroException('Please install ext-zstd to use zstandard compression.');
        }
        $datum = zstd_uncompress($compressed);

        if (false === $datum) {
            throw new AvroException('zstd uncompression failed.');
        }

        return $datum;
    }

    /**
     * @throws AvroException
     */
    private function bzUncompress(string $compressed): string
    {
        if (!extension_loaded('bz2')) {
            throw new AvroException('Please install ext-bz2 to use bzip2 compression.');
        }
        $datum = bzdecompress($compressed);

        if (!is_string($datum)) {
            throw new AvroException('bz2 uncompression failed.');
        }

        return $datum;
    }

    /**
     * @throws AvroException
     */
    private function snappyUncompress(string $compressed): string
    {
        if (!extension_loaded('snappy')) {
            throw new AvroException('Please install ext-snappy to use snappy compression.');
        }
        $crc32 = unpack('N', substr((string) $compressed, -4))[1];
        $datum = snappy_uncompress(substr((string) $compressed, 0, -4));

        if (false === $datum) {
            throw new AvroException('snappy uncompression failed.');
        }

        if ($crc32 !== crc32($datum)) {
            throw new AvroException('snappy uncompression failed - crc32 mismatch.');
        }

        return $datum;
    }
}
