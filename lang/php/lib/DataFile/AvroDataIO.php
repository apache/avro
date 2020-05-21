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

use Apache\Avro\AvroIO;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroFile;
use Apache\Avro\Schema\AvroSchema;

/**
 * @package Avro
 */
class AvroDataIO
{
    /**
     * @var int used in file header
     */
    const VERSION = 1;

    /**
     * @var int count of bytes in synchronization marker
     */
    const SYNC_SIZE = 16;

    /**
     * @var int   count of items per block, arbitrarily set to 4000 * SYNC_SIZE
     * @todo make this value configurable
     */
    const SYNC_INTERVAL = 64000;

    /**
     * @var string map key for datafile metadata codec value
     */
    const METADATA_CODEC_ATTR = 'avro.codec';

    /**
     * @var string map key for datafile metadata schema value
     */
    const METADATA_SCHEMA_ATTR = 'avro.schema';
    /**
     * @var string JSON for datafile metadata schema
     */
    const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /**
     * @var string codec value for NULL codec
     */
    const NULL_CODEC = 'null';

    /**
     * @var string codec value for deflate codec
     */
    const DEFLATE_CODEC = 'deflate';

    const SNAPPY_CODEC = 'snappy';

    const ZSTANDARD_CODEC = 'zstandard';

    /**
     * @var array array of valid codec names
     */
    private static $valid_codecs = [self::NULL_CODEC, self::DEFLATE_CODEC, self::SNAPPY_CODEC, self::ZSTANDARD_CODEC];

    /**
     * @var AvroSchema cached version of metadata schema object
     */
    private static $metadata_schema;

    /**
     * @returns int count of bytes in the initial "magic" segment of the
     *              Avro container file header
     */
    public static function magic_size()
    {
        return strlen(self::magic());
    }

    /**
     * @returns the initial "magic" segment of an Avro container file header.
     */
    public static function magic()
    {
        return ('Obj' . pack('c', self::VERSION));
    }

    /**
     * @returns AvroSchema object of Avro container file metadata.
     */
    public static function metadata_schema()
    {
        if (is_null(self::$metadata_schema)) {
            self::$metadata_schema = AvroSchema::parse(self::METADATA_SCHEMA_JSON);
        }
        return self::$metadata_schema;
    }

    /**
     * @param string $file_path file_path of file to open
     * @param string $mode one of AvroFile::READ_MODE or AvroFile::WRITE_MODE
     * @param string $schema_json JSON of writer's schema
     * @param string $codec compression codec
     * @returns AvroDataIOWriter instance of AvroDataIOWriter
     *
     * @throws AvroDataIOException if $writers_schema is not provided
     *         or if an invalid $mode is given.
     */
    public static function open_file(
        $file_path,
        $mode = AvroFile::READ_MODE,
        $schema_json = null,
        $codec = self::NULL_CODEC
    ) {
        $schema = !is_null($schema_json)
            ? AvroSchema::parse($schema_json) : null;

        $io = false;
        switch ($mode) {
            case AvroFile::WRITE_MODE:
                if (is_null($schema)) {
                    throw new AvroDataIOException('Writing an Avro file requires a schema.');
                }
                $file = new AvroFile($file_path, AvroFile::WRITE_MODE);
                $io = self::open_writer($file, $schema, $codec);
                break;
            case AvroFile::READ_MODE:
                $file = new AvroFile($file_path, AvroFile::READ_MODE);
                $io = self::open_reader($file, $schema);
                break;
            default:
                throw new AvroDataIOException(
                    sprintf("Only modes '%s' and '%s' allowed. You gave '%s'.",
                        AvroFile::READ_MODE, AvroFile::WRITE_MODE, $mode));
        }
        return $io;
    }

    /**
     * @param AvroIO $io
     * @param AvroSchema $schema
     * @param string $codec
     * @returns AvroDataIOWriter
     */
    protected static function open_writer($io, $schema, $codec = self::NULL_CODEC)
    {
        $writer = new AvroIODatumWriter($schema);
        return new AvroDataIOWriter($io, $writer, $schema, $codec);
    }

    /**
     * @param AvroIO $io
     * @param AvroSchema $schema
     * @returns AvroDataIOReader
     */
    protected static function open_reader($io, $schema)
    {
        $reader = new AvroIODatumReader(null, $schema);
        return new AvroDataIOReader($io, $reader);
    }

    /**
     * @param string $codec
     * @returns boolean true if $codec is a valid codec value and false otherwise
     */
    public static function is_valid_codec($codec)
    {
        return in_array($codec, self::valid_codecs());
    }

    /**
     * @returns array array of valid codecs
     */
    public static function valid_codecs()
    {
        return self::$valid_codecs;
    }
}
