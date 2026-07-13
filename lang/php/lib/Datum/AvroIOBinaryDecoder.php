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

namespace Apache\Avro\Datum;

// @todo Implement JSON encoding, as is required by the Avro spec.
use Apache\Avro\Avro;
use Apache\Avro\AvroException;
use Apache\Avro\AvroGMP;
use Apache\Avro\AvroIO;
use Apache\Avro\Schema\AvroArraySchema;
use Apache\Avro\Schema\AvroFixedSchema;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroUnionSchema;

/**
 * Decodes and reads Avro data from an AvroIO object encoded using
 * Avro binary encoding.
 */
class AvroIOBinaryDecoder
{
    /**
     * Reads with a declared length above this many bytes are validated against
     * the number of bytes actually remaining before allocating, to guard
     * against an out-of-memory attack from a malicious or truncated input.
     * Smaller reads skip the check to avoid per-value overhead.
     */
    private const MAX_UNCHECKED_READ = 1048576; // 1 MiB

    /**
     * @param AvroIO $io object from which to read.
     */
    public function __construct(
        private AvroIO $io
    ) {
        Avro::checkPlatform();
    }

    /**
     * @return null
     */
    public function readNull()
    {
        return null;
    }

    public function readBoolean(): bool
    {
        return 1 === ord($this->nextByte());
    }

    /**
     * @param int $len count of bytes to read
     */
    public function read(int $len): string
    {
        if ($len < 0) {
            // AvroStringIO::read() accepts a negative length and moves the
            // pointer backwards; reject it before delegating.
            throw new AvroException("Cannot read a negative number of bytes: {$len}");
        }
        if ($len > self::MAX_UNCHECKED_READ) {
            $remaining = $this->bytesRemaining();
            if ($len > $remaining) {
                throw new AvroException("Cannot read {$len} bytes, only {$remaining} remaining.");
            }
        }

        return $this->io->read($len);
    }

    /**
     * Number of bytes still available to read, determined by seeking to the end
     * and restoring the position (which both the string and file IO
     * implementations support). Used to reject a declared length or collection
     * block count that exceeds the data actually available before allocating.
     */
    public function bytesRemaining(): int
    {
        $current = $this->io->tell();
        $this->io->seek(0, AvroIO::SEEK_END);
        $end = $this->io->tell();
        $this->io->seek($current, AvroIO::SEEK_SET);

        // Clamp to 0: AvroStringIO::seek() allows seeking past EOF, which would
        // otherwise yield a confusing negative "remaining" count.
        return max(0, $end - $current);
    }

    public function readInt(): int
    {
        return (int) $this->readLong();
    }

    public function readLong(): string|int
    {
        $byte = ord($this->nextByte());
        $bytes = [$byte];
        while (0 != ($byte & 0x80)) {
            // A 64-bit value uses at most 10 bytes; reject an overlong varint
            // rather than reading an unbounded continuation chain and silently
            // corrupting the value. Bounds both the native and GMP decode paths.
            if (count($bytes) >= 10) {
                throw new AvroException('Varint is too long');
            }
            $byte = ord($this->nextByte());
            $bytes[] = $byte;
        }

        if (Avro::usesGmp()) {
            return AvroGMP::decodeLongFromArray($bytes);
        }

        return self::decodeLongFromArray($bytes);
    }

    /**
     * @param int[] $bytes array of byte ascii values
     * @return int decoded value
     * @internal Requires 64-bit platform
     */
    public static function decodeLongFromArray(array $bytes): int
    {
        $b = array_shift($bytes);
        $n = $b & 0x7F;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $n |= (($b & 0x7F) << $shift);
            $shift += 7;
        }

        return ($n >> 1) ^ (($n >> 63) << 63) ^ -($n & 1);
    }

    public function readFloat(): float
    {
        return self::intBitsToFloat($this->read(4));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     */
    public static function intBitsToFloat(string $bits): float
    {
        $float = unpack('g', $bits);

        return (float) $float[1];
    }

    public function readDouble(): float
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     */
    public static function longBitsToDouble(string $bits): float
    {
        $double = unpack('e', $bits);

        return (float) $double[1];
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     */
    public function readString(): string
    {
        return $this->readBytes();
    }

    public function readBytes(): string
    {
        return $this->read($this->readLong());
    }

    public function skipNull(): void
    {

    }

    public function skipBoolean(): void
    {
        $this->skip(1);
    }

    /**
     * @param int $len count of bytes to skip
     * @uses AvroIO::seek()
     */
    public function skip(int $len): void
    {
        $this->seek($len, AvroIO::SEEK_CUR);
    }

    public function skipInt(): void
    {
        $this->skipLong();
    }

    public function skipLong(): void
    {
        $b = ord($this->nextByte());
        while (0 != ($b & 0x80)) {
            $b = ord($this->nextByte());
        }
    }

    public function skipFloat(): void
    {
        $this->skip(4);
    }

    public function skipDouble(): void
    {
        $this->skip(8);
    }

    public function skipString(): void
    {
        $this->skipBytes();
    }

    public function skipBytes(): void
    {
        $this->skip($this->readLong());
    }

    public function skipFixed(AvroFixedSchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        $decoder->skip($writersSchema->size());
    }

    public function skipEnum(AvroSchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        $decoder->skipInt();
    }

    public function skipUnion(AvroUnionSchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        $index = $decoder->readLong();
        AvroIODatumReader::skipData($writersSchema->schemaByIndex($index), $decoder);
    }

    public function skipRecord(AvroRecordSchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        foreach ($writersSchema->fields() as $f) {
            AvroIODatumReader::skipData($f->type(), $decoder);
        }
    }

    public function skipArray(AvroArraySchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        $minBytes = AvroIODatumReader::collectionElementMinBytes($writersSchema->items());
        $skipped = 0;
        $blockCount = $decoder->readLong();
        while (0 != $blockCount) {
            $blockSize = null;
            if ($blockCount < 0) {
                if (PHP_INT_MIN == $blockCount) {
                    throw new AvroException('Invalid array block count');
                }
                $blockCount = -$blockCount;
                $blockSize = $decoder->readLong();
                if ($blockSize < 0) {
                    throw new AvroException('Invalid negative array block size');
                }
            }
            // Bound the (normalized) count on both the sized and unsized paths so
            // a negative block count cannot bypass the skip limit.
            AvroIODatumReader::checkSkipCollectionCount($skipped, $blockCount, $minBytes);
            $skipped += $blockCount;
            if (null !== $blockSize) {
                // seek() can move past EOF, so a truncated/oversized block would
                // otherwise be "skipped" silently, hiding truncation. Reject a
                // block size larger than the bytes actually remaining.
                if ($blockSize > $decoder->bytesRemaining()) {
                    throw new AvroException('Array block size exceeds the remaining input');
                }
                if ($minBytes > 0 && $blockCount > intdiv($blockSize, $minBytes)) {
                    throw new AvroException('Array block size too small for the declared element count');
                }
                $decoder->skip($blockSize);
            } else {
                for ($i = 0; $i < $blockCount; $i++) {
                    AvroIODatumReader::skipData($writersSchema->items(), $decoder);
                }
            }
            $blockCount = $decoder->readLong();
        }
    }

    public function skipMap(AvroMapSchema $writersSchema, AvroIOBinaryDecoder $decoder): void
    {
        // Map entries always carry a >= 1 byte key, so the minimum is positive.
        $minBytes = 1 + AvroIODatumReader::collectionElementMinBytes($writersSchema->values());
        $skipped = 0;
        $blockCount = $decoder->readLong();
        while (0 != $blockCount) {
            $blockSize = null;
            if ($blockCount < 0) {
                if (PHP_INT_MIN == $blockCount) {
                    throw new AvroException('Invalid map block count');
                }
                $blockCount = -$blockCount;
                $blockSize = $decoder->readLong();
                if ($blockSize < 0) {
                    throw new AvroException('Invalid negative map block size');
                }
            }
            AvroIODatumReader::checkSkipCollectionCount($skipped, $blockCount, $minBytes);
            $skipped += $blockCount;
            if (null !== $blockSize) {
                // seek() can move past EOF; reject a block size larger than the
                // bytes remaining so a truncated block isn't silently skipped.
                if ($blockSize > $decoder->bytesRemaining()) {
                    throw new AvroException('Map block size exceeds the remaining input');
                }
                if ($minBytes > 0 && $blockCount > intdiv($blockSize, $minBytes)) {
                    throw new AvroException('Map block size too small for the declared element count');
                }
                $decoder->skip($blockSize);
            } else {
                for ($i = 0; $i < $blockCount; $i++) {
                    $decoder->skipString();
                    AvroIODatumReader::skipData($writersSchema->values(), $decoder);
                }
            }
            $blockCount = $decoder->readLong();
        }
    }

    /**
     * @throws AvroException if the next byte cannot be read.
     * @return string the next byte from $this->io.
     */
    private function nextByte(): string
    {
        return $this->read(1);
    }

    /**
     * @uses AvroIO::seek()
     */
    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @return int position of pointer in AvroIO instance
     * @uses AvroIO::tell()
     */
    private function tell(): int
    {
        return $this->io->tell();
    }
}
