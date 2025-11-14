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

namespace Apache\Avro;

/**
 * Barebones IO base class to provide common interface for file and string
 * access within the Avro classes.
 */
interface AvroIO
{
    /**
     * @var string general read mode
     */
    public const READ_MODE = 'r';
    /**
     * @var string general write mode.
     */
    public const WRITE_MODE = 'w';

    /**
     * @var int set position to current index + $offset bytes
     */
    public const SEEK_CUR = SEEK_CUR;
    /**
     * @var int set position equal to $offset bytes
     */
    public const SEEK_SET = SEEK_SET;
    /**
     * @var int set position to end of file + $offset bytes
     */
    public const SEEK_END = SEEK_END;

    /**
     * Read $len bytes from AvroIO instance
     */
    public function read(int $len): string;

    /**
     * Append bytes to this buffer. (Nothing more is needed to support Avro.)
     * @param string $bytes bytes to write
     * @returns int count of bytes written.
     * @throws IO\AvroIOException if $args is not a string value.
     */
    public function write(string $bytes): int;

    /**
     * Return byte offset within AvroIO instance
     */
    public function tell(): int;

    /**
     * Set the position indicator. The new position, measured in bytes
     * from the beginning of the file, is obtained by adding $offset to
     * the position specified by $whence.
     *
     * @param int $whence one of AvroIO::SEEK_SET, AvroIO::SEEK_CUR,
     *                    or Avro::SEEK_END
     * @returns boolean true
     *
     * @throws IO\AvroIOException
     */
    public function seek(int $offset, int $whence = self::SEEK_SET): bool;

    /**
     * Flushes any buffered data to the AvroIO object.
     * @returns bool true upon success.
     */
    public function flush(): bool;

    /**
     * Returns whether or not the current position at the end of this AvroIO
     * instance.
     *
     * Note isEof() is <b>not</b> like eof in C or feof in PHP:
     * it returns TRUE if the *next* read would be end of file,
     * rather than if the *most recent* read read end of file.
     * @returns bool true if at the end of file, and false otherwise
     */
    public function isEof(): bool;

    /**
     * Closes this AvroIO instance.
     */
    public function close(): bool;
}
