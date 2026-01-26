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
 * AvroIO wrapper for string access
 */
class AvroStringIO implements AvroIO, \Stringable
{
    private string $stringBuffer;
    /**
     * @var int  current position in string
     */
    private int $currentIndex;
    /**
     * @var bool whether or not the string is closed.
     */
    private bool $isClosed;

    /**
     * @param string $str initial value of AvroStringIO buffer. Regardless
     *                    of the initial value, the pointer is set to the
     *                    beginning of the buffer.
     */
    public function __construct(string $str = '')
    {
        $this->isClosed = false;
        $this->stringBuffer = $str;
        $this->currentIndex = 0;
    }

    public function __toString(): string
    {
        return $this->stringBuffer;
    }

    /**
     * Append bytes to this buffer.
     * (Nothing more is needed to support Avro.)
     * @param string $bytes bytes to write
     * @throws AvroIOException if $args is not a string value.
     * @return int count of bytes written.
     */
    public function write(string $bytes): int
    {
        $this->checkClosed();

        return $this->appendStr($bytes);
    }

    /**
     * @return bool true if this buffer is closed and false otherwise.
     */
    public function isClosed(): bool
    {
        return $this->isClosed;
    }

    /**
     * @todo test for fencepost errors wrt updating current_index
     * @param mixed $len
     * @return string bytes read from buffer
     */
    public function read($len): string
    {
        $this->checkClosed();
        $read = '';
        for ($i = $this->currentIndex; $i < ($this->currentIndex + $len); $i++) {
            $read .= $this->stringBuffer[$i] ?? '';
        }
        if (strlen($read) < $len) {
            $this->currentIndex = $this->length();
        } else {
            $this->currentIndex += $len;
        }

        return $read;
    }

    /**
     * @return int count of bytes in the buffer
     * @internal Could probably memoize length for performance, but
     *           no need do this yet.
     */
    public function length(): int
    {
        return strlen($this->stringBuffer);
    }

    /**
     * @throws AvroIOException if the seek failed.
     * @return bool true if successful
     */
    public function seek(int $offset, int $whence = self::SEEK_SET): bool
    {
        // Prevent seeking before BOF
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $offset;

                break;
            case self::SEEK_CUR:
                if (0 > $this->currentIndex + $whence) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex += $offset;

                break;
            case self::SEEK_END:
                if (0 > $this->length() + $offset) {
                    throw new AvroIOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $this->length() + $offset;

                break;
            default:
                throw new AvroIOException(sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    /**
     * @see AvroIO::tell()
     */
    public function tell(): int
    {
        return $this->currentIndex;
    }

    /**
     * @see AvroIO::isEof()
     */
    public function isEof(): bool
    {
        return $this->currentIndex >= $this->length();
    }

    /**
     * No-op provided for compatibility with AvroIO interface.
     * @return bool true
     */
    public function flush(): bool
    {
        return true;
    }

    /**
     * Marks this buffer as closed.
     */
    public function close(): bool
    {
        $this->checkClosed();
        $this->isClosed = true;

        return true;
    }

    /**
     * Truncates the truncate buffer to 0 bytes and returns the pointer
     * to the beginning of the buffer.
     * @return bool true
     */
    public function truncate(): bool
    {
        $this->checkClosed();
        $this->stringBuffer = '';
        $this->currentIndex = 0;

        return true;
    }

    /**
     * @uses self::__toString()
     */
    public function string(): string
    {
        return (string) $this;
    }

    /**
     * @throws AvroIOException if the buffer is closed.
     */
    private function checkClosed(): void
    {
        if ($this->isClosed()) {
            throw new AvroIOException('Buffer is closed');
        }
    }

    /**
     * Appends bytes to this buffer.
     * @throws AvroIOException
     * @return int count of bytes written.
     */
    private function appendStr(string $str): int
    {
        $this->checkClosed();
        $this->stringBuffer .= $str;
        $len = strlen($str);
        $this->currentIndex += $len;

        return $len;
    }
}
