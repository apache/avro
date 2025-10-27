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
use Apache\Avro\Datum\Type\AvroDuration;
use Apache\Avro\Schema\AvroArraySchema;
use Apache\Avro\Schema\AvroEnumSchema;
use Apache\Avro\Schema\AvroFixedSchema;
use Apache\Avro\Schema\AvroLogicalType;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;
use Apache\Avro\Schema\AvroUnionSchema;

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
    public function __construct(
        private ?AvroSchema $writers_schema = null,
        private ?AvroSchema $readers_schema = null
    ) {
    }

    public function setWritersSchema(AvroSchema $readers_schema): void
    {
        $this->writers_schema = $readers_schema;
    }

    public function read(AvroIOBinaryDecoder $decoder): mixed
    {
        if (is_null($this->readers_schema)) {
            $this->readers_schema = $this->writers_schema;
        }

        return $this->readData(
            $this->writers_schema,
            $this->readers_schema,
            $decoder
        );
    }

    public function readData(
        AvroSchema $writers_schema,
        AvroSchema $readers_schema,
        AvroIOBinaryDecoder $decoder
    ): mixed {
        // Schema resolution: reader's schema is a union, writer's schema is not
        if (
            $readers_schema instanceof AvroUnionSchema
            && AvroSchema::UNION_SCHEMA === $readers_schema->type()
            && AvroSchema::UNION_SCHEMA !== $writers_schema->type()
        ) {
            foreach ($readers_schema->schemas() as $schema) {
                if (self::schemasMatch($writers_schema, $schema)) {
                    return $this->readData($writers_schema, $schema, $decoder);
                }
            }
            throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
        }

        return match ($writers_schema->type()) {
            AvroSchema::NULL_TYPE => $decoder->readNull(),
            AvroSchema::BOOLEAN_TYPE => $decoder->readBoolean(),
            AvroSchema::INT_TYPE => $decoder->readInt(),
            AvroSchema::LONG_TYPE => $decoder->readLong(),
            AvroSchema::FLOAT_TYPE => $decoder->readFloat(),
            AvroSchema::DOUBLE_TYPE => $decoder->readDouble(),
            AvroSchema::STRING_TYPE => $decoder->readString(),
            AvroSchema::BYTES_TYPE => $this->readBytes($writers_schema, $readers_schema, $decoder->readBytes()),
            AvroSchema::ARRAY_SCHEMA => $this->readArray($writers_schema, $readers_schema, $decoder),
            AvroSchema::MAP_SCHEMA => $this->readMap($writers_schema, $readers_schema, $decoder),
            AvroSchema::UNION_SCHEMA => $this->readUnion($writers_schema, $readers_schema, $decoder),
            AvroSchema::ENUM_SCHEMA => $this->readEnum($writers_schema, $readers_schema, $decoder),
            AvroSchema::FIXED_SCHEMA => $this->readFixed($writers_schema, $readers_schema, $decoder),
            AvroSchema::RECORD_SCHEMA,
            AvroSchema::ERROR_SCHEMA,
            AvroSchema::REQUEST_SCHEMA => $this->readRecord($writers_schema, $readers_schema, $decoder),
            default => throw new AvroException(sprintf(
                "Cannot read unknown schema type: %s",
                $writers_schema->type()
            )),
        };
    }

    /**
     *
     * @returns boolean true if the schemas are consistent with
     *                  each other and false otherwise.
     */
    public static function schemasMatch(
        AvroSchema $writers_schema,
        AvroSchema $readers_schema
    ): bool {
        $writers_schema_type = $writers_schema->type;
        $readers_schema_type = $readers_schema->type;

        if (AvroSchema::UNION_SCHEMA === $writers_schema_type || AvroSchema::UNION_SCHEMA === $readers_schema_type) {
            return true;
        }

        if (AvroSchema::isPrimitiveType($writers_schema_type)) {
            return true;
        }

        switch ($readers_schema_type) {
            case AvroSchema::MAP_SCHEMA:
                return self::attributesMatch(
                    $writers_schema->values(),
                    $readers_schema->values(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::ARRAY_SCHEMA:
                return self::attributesMatch(
                    $writers_schema->items(),
                    $readers_schema->items(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::FIXED_SCHEMA:
                return self::attributesMatch(
                    $writers_schema,
                    $readers_schema,
                    [
                        AvroSchema::FULLNAME_ATTR,
                        AvroSchema::SIZE_ATTR
                    ]
                );
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
                return self::attributesMatch(
                    $writers_schema,
                    $readers_schema,
                    [AvroSchema::FULLNAME_ATTR]
                );
            case AvroSchema::REQUEST_SCHEMA:
                // XXX: This seems wrong
                return true;
            // XXX: no default
        }

        if (
            AvroSchema::INT_TYPE === $writers_schema_type
            && in_array($readers_schema_type, [
                AvroSchema::LONG_TYPE,
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE
            ])
        ) {
            return true;
        }

        if (
            AvroSchema::LONG_TYPE === $writers_schema_type
            && in_array($readers_schema_type, [
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE
            ])
        ) {
            return true;
        }

        if (AvroSchema::FLOAT_TYPE === $writers_schema_type && AvroSchema::DOUBLE_TYPE === $readers_schema_type) {
            return true;
        }

        return false;
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param AvroSchema $schema_one
     * @param AvroSchema $schema_two
     * @param string[] $attribute_names array of string attribute names to compare
     *
     * @return bool true if the attributes match and false otherwise.
     * @throws AvroSchemaParseException
     */
    public static function attributesMatch(
        AvroSchema $schema_one,
        AvroSchema $schema_two,
        array $attribute_names
    ): bool {
        foreach ($attribute_names as $attribute_name) {
            if ($schema_one->attribute($attribute_name) !== $schema_two->attribute($attribute_name)) {
                if ($attribute_name === AvroSchema::FULLNAME_ATTR) {
                    foreach ($schema_two->getAliases() as $alias) {
                        if (
                            $schema_one->attribute($attribute_name) === (new AvroName(
                                $alias,
                                $schema_two->attribute(AvroSchema::NAMESPACE_ATTR),
                                null
                            ))->fullname()
                        ) {
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        return true;
    }

    public function readBytes(AvroSchema $writers_schema, AvroSchema $readers_schema, string $bytes): string
    {
        $logicalTypeWriters = $writers_schema->logicalType();
        if (
            $logicalTypeWriters instanceof AvroLogicalType
            && $logicalTypeWriters->name() === AvroSchema::DECIMAL_LOGICAL_TYPE
        ) {
            if ($logicalTypeWriters !== $readers_schema->logicalType()) {
                throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
            }

            $scale = $logicalTypeWriters->attributes()['scale'] ?? 0;
            $bytes = $this->readDecimal($bytes, $scale);
        }

        return $bytes;
    }

    public function readArray(
        AvroSchema $writers_schema,
        AvroSchema $readers_schema,
        AvroIOBinaryDecoder $decoder
    ): array {
        $items = [];
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $block_count = -$block_count;
                $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $block_count; $i++) {
                $items [] = $this->readData(
                    $writers_schema->items(),
                    $readers_schema->items(),
                    $decoder
                );
            }
            $block_count = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @returns array<int, mixed>
     */
    public function readMap(AvroSchema $writers_schema, AvroSchema $readers_schema, AvroIOBinaryDecoder $decoder): array
    {
        $items = [];
        $pair_count = $decoder->readLong();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                $pair_count = -$pair_count;
                // Note: we're not doing anything with block_size other than skipping it
                $decoder->readLong();
            }

            for ($i = 0; $i < $pair_count; $i++) {
                $key = $decoder->readString();
                $items[$key] = $this->readData(
                    $writers_schema->values(),
                    $readers_schema->values(),
                    $decoder
                );
            }
            $pair_count = $decoder->readLong();
        }
        return $items;
    }

    public function readUnion(AvroSchema $writers_schema, AvroSchema $readers_schema, AvroIOBinaryDecoder $decoder): mixed
    {
        $schema_index = $decoder->readLong();
        $selected_writers_schema = $writers_schema->schemaByIndex($schema_index);
        return $this->readData($selected_writers_schema, $readers_schema, $decoder);
    }

    public function readEnum(
        AvroSchema $writers_schema,
        AvroSchema $readers_schema,
        AvroIOBinaryDecoder $decoder
    ): ?string {
        $symbol_index = $decoder->readInt();
        $symbol = $writers_schema->symbolByIndex($symbol_index);

        if (!$readers_schema->hasSymbol($symbol)) {
            return null;
        } // FIXME: unset wrt schema resolution

        return $symbol;
    }

    public function readFixed(
        AvroSchema $writers_schema,
        AvroSchema $readers_schema,
        AvroIOBinaryDecoder $decoder
    ): string|AvroDuration {
        $logicalTypeWriters = $writers_schema->logicalType();
        if ($logicalTypeWriters instanceof AvroLogicalType) {
            if ($logicalTypeWriters !== $readers_schema->logicalType()) {
                throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
            }

            switch ($logicalTypeWriters->name()) {
                case AvroSchema::DECIMAL_LOGICAL_TYPE:
                    $scale = $logicalTypeWriters->attributes()['scale'] ?? 0;
                    return $this->readDecimal($decoder->readBytes(), $scale);
                case AvroSchema::DURATION_LOGICAL_TYPE:
                    $encodedDuration = $decoder->read($writers_schema->size());
                    if (strlen($encodedDuration) !== 12) {
                        throw new AvroException('Invalid duration fixed size: ' . strlen($encodedDuration));
                    }

                    return AvroDuration::fromBytes($encodedDuration);
                default:
                    throw new AvroException('Unknown logical type for fixed: ' . $logicalTypeWriters->name());
            }
        }

        return $decoder->read($writers_schema->size());
    }

    /**
     * @returns array
     */
    public function readRecord(AvroSchema $writers_schema, AvroSchema $readers_schema, AvroIOBinaryDecoder $decoder)
    {
        $readers_fields = $readers_schema->fieldsHash();
        $record = [];
        foreach ($writers_schema->fields() as $writers_field) {
            $type = $writers_field->type();
            $readers_field = $readers_fields[$writers_field->name()] ?? null;
            if ($readers_field) {
                $record[$writers_field->name()] = $this->readData($type, $readers_field->type(), $decoder);
            } elseif (isset($readers_schema->fieldsByAlias()[$writers_field->name()])) {
                $readers_field = $readers_schema->fieldsByAlias()[$writers_field->name()];
                $field_val = $this->readData($writers_field->type(), $readers_field->type(), $decoder);
                $record[$readers_field->name()] = $field_val;
            } else {
                self::skipData($type, $decoder);
            }
        }
        // Fill in default values
        $writers_fields = $writers_schema->fieldsHash();
        foreach ($readers_fields as $field_name => $field) {
            if (isset($writers_fields[$field_name])) {
                continue;
            }
            if ($field->hasDefaultValue()) {
                $record[$field->name()] = $this->readDefaultValue($field->type(), $field->defaultValue());
            } else {
                null;
            }
        }

        return $record;
    }

    public static function skipData(
        AvroSchema|AvroFixedSchema|AvroEnumSchema|AvroUnionSchema|AvroArraySchema|AvroMapSchema $writers_schema,
        AvroIOBinaryDecoder $decoder
    ): void {
        match ($writers_schema->type()) {
            AvroSchema::NULL_TYPE => $decoder->skipNull(),
            AvroSchema::BOOLEAN_TYPE => $decoder->skipBoolean(),
            AvroSchema::INT_TYPE => $decoder->skipInt(),
            AvroSchema::LONG_TYPE => $decoder->skipLong(),
            AvroSchema::FLOAT_TYPE => $decoder->skipFloat(),
            AvroSchema::DOUBLE_TYPE => $decoder->skipDouble(),
            AvroSchema::STRING_TYPE => $decoder->skipString(),
            AvroSchema::BYTES_TYPE => $decoder->skipBytes(),
            AvroSchema::ARRAY_SCHEMA => $decoder->skipArray($writers_schema, $decoder),
            AvroSchema::MAP_SCHEMA => $decoder->skipMap($writers_schema, $decoder),
            AvroSchema::UNION_SCHEMA => $decoder->skipUnion($writers_schema, $decoder),
            AvroSchema::ENUM_SCHEMA => $decoder->skipEnum($writers_schema, $decoder),
            AvroSchema::FIXED_SCHEMA => $decoder->skipFixed($writers_schema, $decoder),
            AvroSchema::RECORD_SCHEMA, AvroSchema::ERROR_SCHEMA, AvroSchema::REQUEST_SCHEMA => $decoder->skipRecord($writers_schema, $decoder),
            default => throw new AvroException(sprintf(
                'Unknown schema type: %s',
                $writers_schema->type()
            )),
        };
    }

    /**
     * @param null|boolean|int|float|string|array $default_value
     * @return null|boolean|int|float|string|array
     *
     * @throws AvroException if $field_schema type is unknown.
     */
    public function readDefaultValue(AvroSchema $field_schema, mixed $default_value)
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
                return $this->readBytes($field_schema, $field_schema, $default_value);
            case AvroSchema::ARRAY_SCHEMA:
                $array = [];
                foreach ($default_value as $json_val) {
                    $val = $this->readDefaultValue($field_schema->items(), $json_val);
                    $array [] = $val;
                }
                return $array;
            case AvroSchema::MAP_SCHEMA:
                $map = [];
                foreach ($default_value as $key => $json_val) {
                    $map[$key] = $this->readDefaultValue(
                        $field_schema->values(),
                        $json_val
                    );
                }
                return $map;
            case AvroSchema::UNION_SCHEMA:
                return $this->readDefaultValue(
                    $field_schema->schemaByIndex(0),
                    $default_value
                );
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::FIXED_SCHEMA:
                return $default_value;
            case AvroSchema::RECORD_SCHEMA:
                $record = [];
                foreach ($field_schema->fields() as $field) {
                    $field_name = $field->name();
                    if (!$json_val = $default_value[$field_name]) {
                        $json_val = $field->default_value();
                    }

                    $record[$field_name] = $this->readDefaultValue(
                        $field->type(),
                        $json_val
                    );
                }
                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $field_schema->type()));
        }
    }

    private function readDecimal(string $bytes, int $scale): string
    {
        $mostSignificantBit = ord($bytes[0]) & 0x80;
        $padded = str_pad($bytes, 8, $mostSignificantBit ? "\xff" : "\x00", STR_PAD_LEFT);
        $int = unpack('J', $padded)[1];
        return (string) ($scale > 0 ? ($int / (10 ** $scale)) : $int);
    }
}
