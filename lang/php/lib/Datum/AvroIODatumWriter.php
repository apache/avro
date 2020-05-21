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

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroSchema;

/**
 * Handles schema-specific writing of data to the encoder.
 *
 * Ensures that each datum written is consistent with the writer's schema.
 *
 * @package Avro
 */
class AvroIODatumWriter
{
    /**
     * Schema used by this instance to write Avro data.
     * @var AvroSchema
     */
    public $writers_schema;

    /**
     * @param AvroSchema $writers_schema
     */
    function __construct($writers_schema = null)
    {
        $this->writers_schema = $writers_schema;
    }

    /**
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     */
    function write($datum, $encoder)
    {
        $this->write_data($this->writers_schema, $datum, $encoder);
    }

    /**
     * @param AvroSchema $writers_schema
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     * @returns mixed
     *
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     */
    function write_data($writers_schema, $datum, $encoder)
    {
        if (!AvroSchema::is_valid_datum($writers_schema, $datum)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $encoder->write_null($datum);
            case AvroSchema::BOOLEAN_TYPE:
                return $encoder->write_boolean($datum);
            case AvroSchema::INT_TYPE:
                return $encoder->write_int($datum);
            case AvroSchema::LONG_TYPE:
                return $encoder->write_long($datum);
            case AvroSchema::FLOAT_TYPE:
                return $encoder->write_float($datum);
            case AvroSchema::DOUBLE_TYPE:
                return $encoder->write_double($datum);
            case AvroSchema::STRING_TYPE:
                return $encoder->write_string($datum);
            case AvroSchema::BYTES_TYPE:
                return $encoder->write_bytes($datum);
            case AvroSchema::ARRAY_SCHEMA:
                return $this->write_array($writers_schema, $datum, $encoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->write_map($writers_schema, $datum, $encoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->write_fixed($writers_schema, $datum, $encoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->write_enum($writers_schema, $datum, $encoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->write_record($writers_schema, $datum, $encoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->write_union($writers_schema, $datum, $encoder);
            default:
                throw new AvroException(sprintf('Unknown type: %s',
                    $writers_schema->type));
        }
    }

    /**#@+
     * @param AvroSchema $writers_schema
     * @param null|boolean|int|float|string|array $datum item to be written
     * @param AvroIOBinaryEncoder $encoder
     */
    private function write_array($writers_schema, $datum, $encoder)
    {
        $datum_count = count($datum);
        if (0 < $datum_count) {
            $encoder->write_long($datum_count);
            $items = $writers_schema->items();
            foreach ($datum as $item) {
                $this->write_data($items, $item, $encoder);
            }
        }
        return $encoder->write_long(0);
    }

    private function write_map($writers_schema, $datum, $encoder)
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->write_long($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->write_string($k);
                $this->write_data($writers_schema->values(), $v, $encoder);
            }
        }
        $encoder->write_long(0);
    }

    private function write_fixed($writers_schema, $datum, $encoder)
    {
        /**
         * NOTE Unused $writers_schema parameter included for consistency
         * with other write_* methods.
         */
        return $encoder->write($datum);
    }

    private function write_enum($writers_schema, $datum, $encoder)
    {
        $datum_index = $writers_schema->symbol_index($datum);
        return $encoder->write_int($datum_index);
    }

    private function write_record($writers_schema, $datum, $encoder)
    {
        foreach ($writers_schema->fields() as $field) {
            $this->write_data($field->type(), $datum[$field->name()], $encoder);
        }
    }

    private function write_union($writers_schema, $datum, $encoder)
    {
        $datum_schema_index = -1;
        $datum_schema = null;
        foreach ($writers_schema->schemas() as $index => $schema) {
            if (AvroSchema::is_valid_datum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema = $schema;
                break;
            }
        }

        if (is_null($datum_schema)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        $encoder->write_long($datum_schema_index);
        $this->write_data($datum_schema, $datum, $encoder);
    }

    /**#@-*/
}
