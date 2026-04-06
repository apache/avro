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
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

/**
 * Writes Avro data to an AvroIO source using an AvroSchema
 */
class AvroDataIOWriter
{
    /**
     * @var AvroIO object container where data is written
     */
    private AvroIO $io;
    /**
     * @var AvroIOBinaryEncoder encoder for object container
     */
    private AvroIOBinaryEncoder $encoder;
    /**
     * @var AvroStringIO buffer for writing
     */
    private AvroStringIO $buffer;

    private AvroIODatumWriter $datumWriter;

    /**
     * @var AvroIOBinaryEncoder encoder for buffer
     */
    private AvroIOBinaryEncoder $bufferEncoder;
    /**
     * @var int count of items written to block
     */
    private int $blockCount;
    /**
     * @var array<string, mixed> map of object container metadata
     */
    private array $metadata;
    /**
     * @var string compression codec
     */
    private string $codec;
    /**
     * @var string sync marker
     */
    private string $syncMarker;

    public function __construct(
        AvroIO $io,
        AvroIODatumWriter $datumWriter,
        string|AvroSchema|null $writersSchema = null,
        string $codec = AvroDataIO::NULL_CODEC
    ) {
        $this->io = $io;
        $this->datumWriter = $datumWriter;
        $this->encoder = new AvroIOBinaryEncoder($this->io);
        $this->buffer = new AvroStringIO();
        $this->bufferEncoder = new AvroIOBinaryEncoder($this->buffer);
        $this->blockCount = 0;
        $this->metadata = [];

        if ($writersSchema) {
            if (!AvroDataIO::isValidCodec($codec)) {
                throw new AvroDataIOException(
                    sprintf('codec %s is not supported', $codec)
                );
            }

            $this->syncMarker = self::generateSyncMarker();
            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $this->codec = $codec;
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = (string) $writersSchema;
            $this->writeHeader();
        } else {
            $dfr = new AvroDataIOReader($this->io, new AvroIODatumReader());
            $this->syncMarker = $dfr->sync_marker;
            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $this->codec
                = $dfr->metadata[AvroDataIO::METADATA_CODEC_ATTR];
            $schemaFromFile = $dfr->metadata[AvroDataIO::METADATA_SCHEMA_ATTR];
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = $schemaFromFile;
            $this->datumWriter->writersSchema = AvroSchema::parse($schemaFromFile);
            $this->seek(0, SEEK_END);
        }
    }

    public function append(mixed $datum): void
    {
        $this->datumWriter->write($datum, $this->bufferEncoder);
        $this->blockCount++;

        if ($this->buffer->length() >= AvroDataIO::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * Flushes buffer to AvroIO object container and closes it.
     * @see AvroIO::close()
     */
    public function close(): bool
    {
        $this->flush();

        return $this->io->close();
    }

    /**
     * @return string a new, unique sync marker.
     */
    private static function generateSyncMarker(): string
    {
        // From https://php.net/manual/en/function.mt-rand.php comments
        return pack(
            'S8',
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF) | 0x4000,
            random_int(0, 0xFFFF) | 0x8000,
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF),
            random_int(0, 0xFFFF)
        );
    }

    /**
     * Writes the header of the AvroIO object container
     */
    private function writeHeader(): void
    {
        $this->write(AvroDataIO::magic());
        $this->datumWriter->writeData(
            AvroDataIO::metadataSchema(),
            $this->metadata,
            $this->encoder
        );
        $this->write($this->syncMarker);
    }

    /**
     * @uses AvroIO::write()
     */
    private function write(string $bytes): int
    {
        return $this->io->write($bytes);
    }

    /**
     * @uses AvroIO::seek()
     */
    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * Writes a block of data to the AvroIO object container.
     */
    private function writeBlock(): void
    {
        if ($this->blockCount > 0) {
            $this->encoder->writeLong($this->blockCount);
            $toWrite = (string) $this->buffer;

            $toWrite = match ($this->codec) {
                AvroDataIO::DEFLATE_CODEC => $this->gzCompress($toWrite),
                AvroDataIO::ZSTANDARD_CODEC => $this->zstdCompress($toWrite),
                AvroDataIO::SNAPPY_CODEC => $this->snappyCompress($toWrite),
                AvroDataIO::BZIP2_CODEC => $this->bzCompress($toWrite),
                default => $toWrite,
            };

            $this->encoder->writeLong(strlen($toWrite));
            $this->write($toWrite);
            $this->write($this->syncMarker);
            $this->buffer->truncate();
            $this->blockCount = 0;
        }
    }

    /**
     * Flushes biffer to AvroIO object container.
     * @see AvroIO::flush()
     */
    private function flush(): void
    {
        $this->writeBlock();
        $this->io->flush();
    }

    /**
     * @throws AvroException
     */
    private function gzCompress(string $data): string
    {
        $data = gzdeflate($data);
        if (false === $data) {
            throw new AvroException('gzip compression failed.');
        }

        return $data;
    }

    /**
     * @throws AvroException
     */
    private function zstdCompress(string $data): string
    {
        if (!extension_loaded('zstd')) {
            throw new AvroException('Please install ext-zstd to use zstandard compression.');
        }
        $data = zstd_compress($data);

        if (false === $data) {
            throw new AvroException('zstd compression failed.');
        }

        return $data;
    }

    /**
     * @throws AvroException
     */
    private function snappyCompress(string $data): string
    {
        if (!extension_loaded('snappy')) {
            throw new AvroException('Please install ext-snappy to use snappy compression.');
        }
        $crc32 = crc32($data);
        $compressed = snappy_compress($data);
        if (false === $compressed) {
            throw new AvroException('snappy compression failed.');
        }

        return pack('a*N', $compressed, $crc32);
    }

    /**
     * @throws AvroException
     */
    private function bzCompress(string $toWrite): string
    {
        if (!extension_loaded('bz2')) {
            throw new AvroException('Please install ext-bz2 to use bzip2 compression.');
        }
        $toWrite = bzcompress($toWrite);

        if (is_int($toWrite)) {
            throw new AvroException("bz2 compression failed (error: {$toWrite}).");
        }

        return $toWrite;
    }
}
