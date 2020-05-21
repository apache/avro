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
 * Handles schema-specifc reading of data from the decoder.
 *
 * Also handles schema resolution between the reader and writer
 * schemas (if a writer's schema is provided).
 *
 * @package Avro
 */
class AvroIODatumReader
{
    /**
     * @var AvroSchema
     */
    private $writers_schema;
    /**
     * @var AvroSchema
     */
    private $readers_schema;

    /**
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     */
    public function __construct($writers_schema = null, $readers_schema = null)
    {
        $this->writers_schema = $writers_schema;
        $this->readers_schema = $readers_schema;
    }

    /**
     * @param AvroSchema $readers_schema
     */
    public function set_writers_schema($readers_schema)
    {
        $this->writers_schema = $readers_schema;
    }

    /**
     * @param AvroIOBinaryDecoder $decoder
     * @returns string
     */
    public function read($decoder)
    {
        if (is_null($this->readers_schema)) {
            $this->readers_schema = $this->writers_schema;
        }
        return $this->read_data($this->writers_schema, $this->readers_schema,
            $decoder);
    }

    /**
     * @returns mixed
     */
    public function read_data($writers_schema, $readers_schema, $decoder)
    {
        if (!self::schemas_match($writers_schema, $readers_schema)) {
            throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
        }

        // Schema resolution: reader's schema is a union, writer's schema is not
        if (AvroSchema::UNION_SCHEMA == $readers_schema->type()
            && AvroSchema::UNION_SCHEMA != $writers_schema->type()) {
            foreach ($readers_schema->schemas() as $schema) {
                if (self::schemas_match($writers_schema, $schema)) {
                    return $this->read_data($writers_schema, $schema, $decoder);
                }
            }
            throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
        }

        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->read_null();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->read_boolean();
            case AvroSchema::INT_TYPE:
                return $decoder->read_int();
            case AvroSchema::LONG_TYPE:
                return $decoder->read_long();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->read_float();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->read_double();
            case AvroSchema::STRING_TYPE:
                return $decoder->read_string();
            case AvroSchema::BYTES_TYPE:
                return $decoder->read_bytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $this->read_array($writers_schema, $readers_schema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->read_map($writers_schema, $readers_schema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->read_union($writers_schema, $readers_schema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->read_enum($writers_schema, $readers_schema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->read_fixed($writers_schema, $readers_schema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->read_record($writers_schema, $readers_schema, $decoder);
            default:
                throw new AvroException(sprintf("Cannot read unknown schema type: %s",
                    $writers_schema->type()));
        }
    }

    /**
     *
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     * @returns boolean true if the schemas are consistent with
     *                  each other and false otherwise.
     */
    public static function schemas_match($writers_schema, $readers_schema)
    {
        $writers_schema_type = $writers_schema->type;
        $readers_schema_type = $readers_schema->type;

        if (AvroSchema::UNION_SCHEMA == $writers_schema_type
            || AvroSchema::UNION_SCHEMA == $readers_schema_type) {
            return true;
        }

        if ($writers_schema_type == $readers_schema_type) {
            if (AvroSchema::is_primitive_type($writers_schema_type)) {
                return true;
            }

            switch ($readers_schema_type) {
                case AvroSchema::MAP_SCHEMA:
                    return self::attributes_match($writers_schema->values(),
                        $readers_schema->values(),
                        array(AvroSchema::TYPE_ATTR));
                case AvroSchema::ARRAY_SCHEMA:
                    return self::attributes_match($writers_schema->items(),
                        $readers_schema->items(),
                        array(AvroSchema::TYPE_ATTR));
                case AvroSchema::ENUM_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(AvroSchema::FULLNAME_ATTR));
                case AvroSchema::FIXED_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(
                            AvroSchema::FULLNAME_ATTR,
                            AvroSchema::SIZE_ATTR
                        ));
                case AvroSchema::RECORD_SCHEMA:
                case AvroSchema::ERROR_SCHEMA:
                    return self::attributes_match($writers_schema, $readers_schema,
                        array(AvroSchema::FULLNAME_ATTR));
                case AvroSchema::REQUEST_SCHEMA:
                    // XXX: This seems wrong
                    return true;
                // XXX: no default
            }

            if (AvroSchema::INT_TYPE == $writers_schema_type
                && in_array($readers_schema_type, array(
                    AvroSchema::LONG_TYPE,
                    AvroSchema::FLOAT_TYPE,
                    AvroSchema::DOUBLE_TYPE
                ))) {
                return true;
            }

            if (AvroSchema::LONG_TYPE == $writers_schema_type
                && in_array($readers_schema_type, array(
                    AvroSchema::FLOAT_TYPE,
                    AvroSchema::DOUBLE_TYPE
                ))) {
                return true;
            }

            if (AvroSchema::FLOAT_TYPE == $writers_schema_type
                && AvroSchema::DOUBLE_TYPE == $readers_schema_type) {
                return true;
            }

            return false;
        }
    }

    /**#@+
     * @param AvroSchema $writers_schema
     * @param AvroSchema $readers_schema
     * @param AvroIOBinaryDecoder $decoder
     */

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param AvroSchema $schema_one
     * @param AvroSchema $schema_two
     * @param string[] $attribute_names array of string attribute names to compare
     *
     * @returns boolean true if the attributes match and false otherwise.
     */
    static function attributes_match($schema_one, $schema_two, $attribute_names)
    {
        foreach ($attribute_names as $attribute_name) {
            if ($schema_one->attribute($attribute_name)
                !== $schema_two->attribute($attribute_name)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @returns array
     */
    public function read_array($writers_schema, $readers_schema, $decoder)
    {
        $items = array();
        $block_count = $decoder->read_long();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $block_count = -$block_count;
                $block_size = $decoder->read_long(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $block_count; $i++) {
                $items [] = $this->read_data($writers_schema->items(),
                    $readers_schema->items(),
                    $decoder);
            }
            $block_count = $decoder->read_long();
        }
        return $items;
    }

    /**
     * @returns array
     */
    public function read_map($writers_schema, $readers_schema, $decoder)
    {
        $items = array();
        $pair_count = $decoder->read_long();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                $pair_count = -$pair_count;
                // Note: we're not doing anything with block_size other than skipping it
                $block_size = $decoder->read_long();
            }

            for ($i = 0; $i < $pair_count; $i++) {
                $key = $decoder->read_string();
                $items[$key] = $this->read_data($writers_schema->values(),
                    $readers_schema->values(),
                    $decoder);
            }
            $pair_count = $decoder->read_long();
        }
        return $items;
    }

    /**
     * @returns mixed
     */
    public function read_union($writers_schema, $readers_schema, $decoder)
    {
        $schema_index = $decoder->read_long();
        $selected_writers_schema = $writers_schema->schema_by_index($schema_index);
        return $this->read_data($selected_writers_schema, $readers_schema, $decoder);
    }

    /**
     * @returns string
     */
    public function read_enum($writers_schema, $readers_schema, $decoder)
    {
        $symbol_index = $decoder->read_int();
        $symbol = $writers_schema->symbol_by_index($symbol_index);
        if (!$readers_schema->has_symbol($symbol)) {
            null;
        } // FIXME: unset wrt schema resolution
        return $symbol;
    }

    /**
     * @returns string
     */
    public function read_fixed($writers_schema, $readers_schema, $decoder)
    {
        return $decoder->read($writers_schema->size());
    }

    /**
     * @returns array
     */
    public function read_record($writers_schema, $readers_schema, $decoder)
    {
        $readers_fields = $readers_schema->fields_hash();
        $record = array();
        foreach ($writers_schema->fields() as $writers_field) {
            $type = $writers_field->type();
            if (isset($readers_fields[$writers_field->name()])) {
                $record[$writers_field->name()]
                    = $this->read_data($type,
                    $readers_fields[$writers_field->name()]->type(),
                    $decoder);
            } else {
                $this->skip_data($type, $decoder);
            }
        }
        // Fill in default values
        if (count($readers_fields) > count($record)) {
            $writers_fields = $writers_schema->fields_hash();
            foreach ($readers_fields as $field_name => $field) {
                if (!isset($writers_fields[$field_name])) {
                    if ($field->has_default_value()) {
                        $record[$field->name()]
                            = $this->read_default_value($field->type(),
                            $field->default_value());
                    } else {
                        null;
                    } // FIXME: unset
                }
            }
        }

        return $record;
    }
    /**#@-*/

    /**
     * @param AvroSchema $writers_schema
     * @param AvroIOBinaryDecoder $decoder
     */
    private function skip_data($writers_schema, $decoder)
    {
        switch ($writers_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->skip_null();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->skip_boolean();
            case AvroSchema::INT_TYPE:
                return $decoder->skip_int();
            case AvroSchema::LONG_TYPE:
                return $decoder->skip_long();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->skip_float();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->skip_double();
            case AvroSchema::STRING_TYPE:
                return $decoder->skip_string();
            case AvroSchema::BYTES_TYPE:
                return $decoder->skip_bytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $decoder->skip_array($writers_schema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $decoder->skip_map($writers_schema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $decoder->skip_union($writers_schema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $decoder->skip_enum($writers_schema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $decoder->skip_fixed($writers_schema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $decoder->skip_record($writers_schema, $decoder);
            default:
                throw new AvroException(sprintf('Unknown schema type: %s',
                    $writers_schema->type()));
        }
    }

    /**
     * @param AvroSchema $field_schema
     * @param null|boolean|int|float|string|array $default_value
     * @returns null|boolean|int|float|string|array
     *
     * @throws AvroException if $field_schema type is unknown.
     */
    public function read_default_value($field_schema, $default_value)
    {
        switch ($field_schema->type()) {
            case AvroSchema::NULL_TYPE:
                return null;
            case AvroSchema::BOOLEAN_TYPE:
                return $default_value;
            case AvroSchema::INT_TYPE:
            case AvroSchema::LONG_TYPE:
                return (int) $default_value;
            case AvroSchema::FLOAT_TYPE:
            case AvroSchema::DOUBLE_TYPE:
                return (float) $default_value;
            case AvroSchema::STRING_TYPE:
            case AvroSchema::BYTES_TYPE:
                return $default_value;
            case AvroSchema::ARRAY_SCHEMA:
                $array = array();
                foreach ($default_value as $json_val) {
                    $val = $this->read_default_value($field_schema->items(), $json_val);
                    $array [] = $val;
                }
                return $array;
            case AvroSchema::MAP_SCHEMA:
                $map = array();
                foreach ($default_value as $key => $json_val) {
                    $map[$key] = $this->read_default_value($field_schema->values(),
                        $json_val);
                }
                return $map;
            case AvroSchema::UNION_SCHEMA:
                return $this->read_default_value($field_schema->schema_by_index(0),
                    $default_value);
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::FIXED_SCHEMA:
                return $default_value;
            case AvroSchema::RECORD_SCHEMA:
                $record = array();
                foreach ($field_schema->fields() as $field) {
                    $field_name = $field->name();
                    if (!$json_val = $default_value[$field_name]) {
                        $json_val = $field->default_value();
                    }

                    $record[$field_name] = $this->read_default_value($field->type(),
                        $json_val);
                }
                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $field_schema->type()));
        }
    }
}
