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

declare(strict_types=1);

namespace Apache\Avro\IO;

use Apache\Avro\AvroIO;

/**
 * AvroIO wrapper for PHP file access functions
 */
class AvroFile implements AvroIO
{
    /**
     * @var string fopen read mode value. Used internally.
     */
    public const FOPEN_READ_MODE = 'rb';

    /**
     * @var string fopen write mode value. Used internally.
     */
    public const FOPEN_WRITE_MODE = 'wb';

    /**
     * @var resource file handle for AvroFile instance
     */
    private $fileHandle;

    public function __construct(
        private string $filePath,
        string $mode = self::READ_MODE
    ) {
        switch ($mode) {
            case self::WRITE_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_WRITE_MODE);
                if (false === $this->fileHandle) {
                    throw new AvroIOException('Could not open file for writing');
                }

                break;
            case self::READ_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_READ_MODE);
                if (false === $this->fileHandle) {
                    throw new AvroIOException('Could not open file for reading');
                }

                break;
            default:
                throw new AvroIOException(
                    sprintf(
                        "Only modes '%s' and '%s' allowed. You provided '%s'.",
                        self::READ_MODE,
                        self::WRITE_MODE,
                        $mode
                    )
                );
        }
    }

    /**
     * @throws AvroIOException if write failed.
     * @return int count of bytes written
     */
    public function write(string $bytes): int
    {
        $len = fwrite($this->fileHandle, $bytes);
        if (false === $len) {
            throw new AvroIOException(sprintf('Could not write to file'));
        }

        return $len;
    }

    /**
     * @throws AvroIOException if tell failed.
     * @return int current position within the file
     */
    public function tell(): int
    {
        $position = ftell($this->fileHandle);
        if (false === $position) {
            throw new AvroIOException('Could not execute tell on reader');
        }

        return $position;
    }

    /**
     * Closes the file.
     * @throws AvroIOException if there was an error closing the file.
     * @return bool true if successful.
     */
    public function close(): bool
    {
        $res = fclose($this->fileHandle);
        if (false === $res) {
            throw new AvroIOException('Error closing file.');
        }

        return $res;
    }

    /**
     * @return bool true if the pointer is at the end of the file,
     *                  and false otherwise.
     * @see AvroIO::isEof() as behavior differs from feof()
     */
    public function isEof(): bool
    {
        $this->read(1);
        if (feof($this->fileHandle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    /**
     * @param int $len count of bytes to read.
     * @throws AvroIOException if length value is negative or if the read failed
     * @return string bytes read
     */
    public function read(int $len): string
    {
        if (0 > $len) {
            throw new AvroIOException(
                sprintf("Invalid length value passed to read: %d", $len)
            );
        }

        if (0 === $len) {
            return '';
        }

        $bytes = fread($this->fileHandle, $len);
        if (false === $bytes) {
            throw new AvroIOException('Could not read from file');
        }

        return $bytes;
    }

    /**
     * @throws AvroIOException if seek failed.
     * @return bool true upon success
     * @see AvroIO::seek()
     */
    public function seek(int $offset, int $whence = SEEK_SET): bool
    {
        $res = fseek($this->fileHandle, $offset, $whence);
        // Note: does not catch seeking beyond end of file
        if (-1 === $res) {
            throw new AvroIOException(
                sprintf(
                    "Could not execute seek (offset = %d, whence = %d)",
                    $offset,
                    $whence
                )
            );
        }

        return true;
    }

    /**
     * @throws AvroIOException if there was an error flushing the file.
     * @return bool true if the flush was successful.
     */
    public function flush(): bool
    {
        $res = fflush($this->fileHandle);
        if (false === $res) {
            throw new AvroIOException('Could not flush file.');
        }

        return true;
    }
}
