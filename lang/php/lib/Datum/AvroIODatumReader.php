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
use Apache\Avro\Schema\AvroAliasedSchema;
use Apache\Avro\Schema\AvroArraySchema;
use Apache\Avro\Schema\AvroEnumSchema;
use Apache\Avro\Schema\AvroFixedSchema;
use Apache\Avro\Schema\AvroLogicalType;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;
use Apache\Avro\Schema\AvroUnionSchema;

/**
 * Handles schema-specifc reading of data from the decoder.
 *
 * Also handles schema resolution between the reader and writer
 * schemas (if a writer's schema is provided).
 */
class AvroIODatumReader
{
    /**
     * Name of the environment variable overriding the maximum number of
     * zero-byte-encoded collection elements (e.g. an array of nulls) to
     * allocate from a single decode.
     */
    public const MAX_COLLECTION_ITEMS_ENV = 'AVRO_MAX_COLLECTION_ITEMS';

    /**
     * Default maximum number of zero-byte-encoded collection elements to
     * allocate. Such elements consume no input, so the bytes-remaining check
     * cannot bound their count; without a cap a tiny payload can declare a huge
     * block count and exhaust memory. Overridable with the
     * {@see self::MAX_COLLECTION_ITEMS_ENV} environment variable.
     */
    public const DEFAULT_MAX_COLLECTION_ITEMS = 10000000;

    /**
     * Structural cap on the number of elements in any array or map (an overflow
     * / defense-in-depth guard), matching the historical Integer.MAX_VALUE - 8
     * limit. Non-zero-byte elements are also bounded by the bytes remaining.
     */
    public const DEFAULT_MAX_COLLECTION_STRUCTURAL = 2147483639;

    public function __construct(
        private ?AvroSchema $writersSchema = null,
        private ?AvroSchema $readersSchema = null
    ) {
    }

    public function setWritersSchema(AvroSchema $schema): void
    {
        $this->writersSchema = $schema;
    }

    public function read(AvroIOBinaryDecoder $decoder): mixed
    {
        if (is_null($this->readersSchema)) {
            $this->readersSchema = $this->writersSchema;
        }

        return $this->readData(
            $this->writersSchema,
            $this->readersSchema,
            $decoder
        );
    }

    public function readData(
        AvroSchema $writersSchema,
        AvroSchema $readersSchema,
        AvroIOBinaryDecoder $decoder
    ): mixed {
        // Schema resolution: reader's schema is a union, writer's schema is not
        if (
            $readersSchema instanceof AvroUnionSchema
            && AvroSchema::UNION_SCHEMA === $readersSchema->type()
            && AvroSchema::UNION_SCHEMA !== $writersSchema->type()
        ) {
            foreach ($readersSchema->schemas() as $schema) {
                if (self::schemasMatch($writersSchema, $schema)) {
                    return $this->readData($writersSchema, $schema, $decoder);
                }
            }

            throw new AvroIOSchemaMatchException($writersSchema, $readersSchema);
        }

        return match ($writersSchema->type()) {
            AvroSchema::NULL_TYPE => $decoder->readNull(),
            AvroSchema::BOOLEAN_TYPE => $decoder->readBoolean(),
            AvroSchema::INT_TYPE => $decoder->readInt(),
            AvroSchema::LONG_TYPE => $decoder->readLong(),
            AvroSchema::FLOAT_TYPE => $decoder->readFloat(),
            AvroSchema::DOUBLE_TYPE => $decoder->readDouble(),
            AvroSchema::STRING_TYPE => $decoder->readString(),
            AvroSchema::BYTES_TYPE => $this->readBytes($writersSchema, $readersSchema, $decoder->readBytes()),
            AvroSchema::ARRAY_SCHEMA => $this->readArray($writersSchema, $readersSchema, $decoder),
            AvroSchema::MAP_SCHEMA => $this->readMap($writersSchema, $readersSchema, $decoder),
            AvroSchema::UNION_SCHEMA => $this->readUnion($writersSchema, $readersSchema, $decoder),
            AvroSchema::ENUM_SCHEMA => $this->readEnum($writersSchema, $readersSchema, $decoder),
            AvroSchema::FIXED_SCHEMA => $this->readFixed($writersSchema, $readersSchema, $decoder),
            AvroSchema::RECORD_SCHEMA,
            AvroSchema::ERROR_SCHEMA,
            AvroSchema::REQUEST_SCHEMA => $this->readRecord($writersSchema, $readersSchema, $decoder),
            default => throw new AvroException(sprintf(
                "Cannot read unknown schema type: %s",
                $writersSchema->type()
            )),
        };
    }

    /**
     * @throws AvroSchemaParseException
     * @return bool true if the schemas are consistent with each other and false otherwise.
     */
    public static function schemasMatch(
        AvroSchema $writersSchema,
        AvroSchema $readersSchema
    ): bool {
        $writersSchemaType = $writersSchema->type;
        $readersSchemaType = $readersSchema->type;

        if (AvroSchema::UNION_SCHEMA === $writersSchemaType || AvroSchema::UNION_SCHEMA === $readersSchemaType) {
            return true;
        }

        if (
            $writersSchema instanceof AvroPrimitiveSchema
            && $readersSchema instanceof AvroPrimitiveSchema
            && $writersSchemaType === $readersSchemaType
        ) {
            return true;
        }

        switch ($readersSchemaType) {
            case AvroSchema::MAP_SCHEMA:
                if (
                    !$writersSchema instanceof AvroMapSchema
                    || !$readersSchema instanceof AvroMapSchema
                ) {
                    return false;
                }

                return self::attributesMatch(
                    $writersSchema->values(),
                    $readersSchema->values(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::ARRAY_SCHEMA:
                if (
                    !$writersSchema instanceof AvroArraySchema
                    || !$readersSchema instanceof AvroArraySchema
                ) {
                    return false;
                }

                return self::attributesMatch(
                    $writersSchema->items(),
                    $readersSchema->items(),
                    [AvroSchema::TYPE_ATTR]
                );
            case AvroSchema::FIXED_SCHEMA:
                return self::attributesMatch(
                    $writersSchema,
                    $readersSchema,
                    [
                        AvroSchema::FULLNAME_ATTR,
                        AvroSchema::SIZE_ATTR,
                    ]
                );
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
                return self::attributesMatch(
                    $writersSchema,
                    $readersSchema,
                    [AvroSchema::FULLNAME_ATTR]
                );
            case AvroSchema::REQUEST_SCHEMA:
                // XXX: This seems wrong
                return true;
                // XXX: no default
        }

        if (
            AvroSchema::INT_TYPE === $writersSchemaType
            && in_array($readersSchemaType, [
                AvroSchema::LONG_TYPE,
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE,
            ])
        ) {
            return true;
        }

        if (
            AvroSchema::LONG_TYPE === $writersSchemaType
            && in_array($readersSchemaType, [
                AvroSchema::FLOAT_TYPE,
                AvroSchema::DOUBLE_TYPE,
            ])
        ) {
            return true;
        }

        if (AvroSchema::FLOAT_TYPE === $writersSchemaType && AvroSchema::DOUBLE_TYPE === $readersSchemaType) {
            return true;
        }

        return false;
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param string[] $attribute_names array of string attribute names to compare
     *
     * @throws AvroSchemaParseException
     * @return bool true if the attributes match and false otherwise.
     */
    public static function attributesMatch(
        AvroSchema $schema_one,
        AvroSchema $schema_two,
        array $attribute_names
    ): bool {
        foreach ($attribute_names as $attribute_name) {
            if ($schema_one->attribute($attribute_name) !== $schema_two->attribute($attribute_name)) {
                if (AvroSchema::FULLNAME_ATTR === $attribute_name) {

                    if (
                        !$schema_two instanceof AvroAliasedSchema
                    ) {
                        return false;
                    }

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
            && AvroSchema::DECIMAL_LOGICAL_TYPE === $logicalTypeWriters->name()
        ) {
            if ($logicalTypeWriters !== $readers_schema->logicalType()) {
                throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
            }

            $scale = $logicalTypeWriters->attributes()['scale'] ?? 0;
            $bytes = $this->readDecimal($bytes, $scale);
        }

        return $bytes;
    }

    /**
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     * @return list<mixed>
     */
    public function readArray(
        AvroArraySchema $writersSchema,
        AvroArraySchema $readersSchema,
        AvroIOBinaryDecoder $decoder
    ): array {
        $items = [];
        $minBytes = self::minBytesPerElement($writersSchema->items());
        $blockCount = $decoder->readLong();
        while (0 != $blockCount) {
            if ($blockCount < 0) {
                // PHP_INT_MIN cannot be negated: -PHP_INT_MIN promotes to a
                // float, so reject it rather than propagating a non-int count.
                if (PHP_INT_MIN == $blockCount) {
                    throw new AvroException('Invalid array block count');
                }
                $blockCount = -$blockCount;
                $blockSize = $decoder->readLong(); // block byte-size
                if ($blockSize < 0) {
                    throw new AvroException('Invalid negative array block size');
                }
            }
            self::ensureCollectionAvailable($decoder, count($items), $blockCount, $minBytes);
            for ($i = 0; $i < $blockCount; $i++) {
                $items[] = $this->readData(
                    $writersSchema->items(),
                    $readersSchema->items(),
                    $decoder
                );
            }
            $blockCount = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @return array<string, mixed>
     */
    public function readMap(
        AvroMapSchema $writersSchema,
        AvroMapSchema $readersSchema,
        AvroIOBinaryDecoder $decoder
    ): array {
        $items = [];
        $minBytes = 1 + self::minBytesPerElement($writersSchema->values());
        $read = 0; // Cumulative pairs read; count($items) would undercount duplicate keys.
        $pair_count = $decoder->readLong();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                // PHP_INT_MIN cannot be negated: -PHP_INT_MIN promotes to a
                // float, so reject it rather than propagating a non-int count.
                if (PHP_INT_MIN == $pair_count) {
                    throw new AvroException('Invalid map block count');
                }
                $pair_count = -$pair_count;
                $blockSize = $decoder->readLong(); // block byte-size
                if ($blockSize < 0) {
                    throw new AvroException('Invalid negative map block size');
                }
            }

            // Map keys are strings (>= 1 byte length prefix) plus the value.
            // Bound against the cumulative pairs read, not count($items): a
            // stream repeating the same key would otherwise shrink count($items)
            // and slip past the cumulative cap.
            self::ensureCollectionAvailable($decoder, $read, $pair_count, $minBytes);
            $read += $pair_count;
            for ($i = 0; $i < $pair_count; $i++) {
                $key = $decoder->readString();
                $items[$key] = $this->readData(
                    $writersSchema->values(),
                    $readersSchema->values(),
                    $decoder
                );
            }
            $pair_count = $decoder->readLong();
        }

        return $items;
    }

    public function readUnion(
        AvroUnionSchema $writers_schema,
        AvroUnionSchema $readers_schema,
        AvroIOBinaryDecoder $decoder
    ): mixed {
        $schema_index = $decoder->readLong();
        $selected_writers_schema = $writers_schema->schemaByIndex($schema_index);

        return $this->readData($selected_writers_schema, $readers_schema, $decoder);
    }

    public function readEnum(
        AvroEnumSchema $writers_schema,
        AvroEnumSchema $readers_schema,
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
        AvroFixedSchema $writers_schema,
        AvroFixedSchema $readers_schema,
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
                    if (12 !== strlen($encodedDuration)) {
                        throw new AvroException('Invalid duration fixed size: '.strlen($encodedDuration));
                    }

                    return AvroDuration::fromBytes($encodedDuration);
                default:
                    throw new AvroException('Unknown logical type for fixed: '.$logicalTypeWriters->name());
            }
        }

        return $decoder->read($writers_schema->size());
    }

    /**
     * @return array<string, mixed>
     */
    public function readRecord(
        AvroRecordSchema $writersSchema,
        AvroRecordSchema $readersSchema,
        AvroIOBinaryDecoder $decoder
    ): array {
        $readerFields = $readersSchema->fieldsHash();
        $record = [];
        foreach ($writersSchema->fields() as $writersField) {
            $type = $writersField->type();
            $readersField = $readerFields[$writersField->name()] ?? null;
            if ($readersField) {
                $record[$writersField->name()] = $this->readData($type, $readersField->type(), $decoder);
            } elseif (isset($readersSchema->fieldsByAlias()[$writersField->name()])) {
                $readersField = $readersSchema->fieldsByAlias()[$writersField->name()];
                $field_val = $this->readData($writersField->type(), $readersField->type(), $decoder);
                $record[$readersField->name()] = $field_val;
            } else {
                self::skipData($type, $decoder);
            }
        }
        // Fill in default values
        $writers_fields = $writersSchema->fieldsHash();
        foreach ($readerFields as $field_name => $field) {
            if (isset($writers_fields[$field_name])) {
                continue;
            }
            if ($field->hasDefaultValue()) {
                $record[$field->name()] = $this->readDefaultValue($field->type(), $field->defaultValue());
            }
        }

        return $record;
    }

    public static function skipData(
        AvroSchema|AvroFixedSchema|AvroEnumSchema|AvroUnionSchema|AvroArraySchema|AvroMapSchema $writersSchema,
        AvroIOBinaryDecoder $decoder
    ): void {
        match ($writersSchema->type()) {
            AvroSchema::NULL_TYPE => $decoder->skipNull(),
            AvroSchema::BOOLEAN_TYPE => $decoder->skipBoolean(),
            AvroSchema::INT_TYPE => $decoder->skipInt(),
            AvroSchema::LONG_TYPE => $decoder->skipLong(),
            AvroSchema::FLOAT_TYPE => $decoder->skipFloat(),
            AvroSchema::DOUBLE_TYPE => $decoder->skipDouble(),
            AvroSchema::STRING_TYPE => $decoder->skipString(),
            AvroSchema::BYTES_TYPE => $decoder->skipBytes(),
            AvroSchema::ARRAY_SCHEMA => $decoder->skipArray($writersSchema, $decoder),
            AvroSchema::MAP_SCHEMA => $decoder->skipMap($writersSchema, $decoder),
            AvroSchema::UNION_SCHEMA => $decoder->skipUnion($writersSchema, $decoder),
            AvroSchema::ENUM_SCHEMA => $decoder->skipEnum($writersSchema, $decoder),
            AvroSchema::FIXED_SCHEMA => $decoder->skipFixed($writersSchema, $decoder),
            AvroSchema::RECORD_SCHEMA,
            AvroSchema::ERROR_SCHEMA,
            AvroSchema::REQUEST_SCHEMA => $decoder->skipRecord($writersSchema, $decoder),
            default => throw new AvroException(sprintf(
                'Unknown schema type: %s',
                $writersSchema->type()
            )),
        };
    }

    /**
     * @param null|array<string, mixed>|bool|float|int|list<mixed>|string $defaultValue
     *
     * @throws AvroException if $field_schema type is unknown.
     * @return null|array<string, mixed>|bool|float|int|list<mixed>|string
     */
    public function readDefaultValue(AvroSchema $fieldSchema, mixed $defaultValue): mixed
    {
        switch ($fieldSchema->type()) {
            case AvroSchema::NULL_TYPE:
                return null;
            case AvroSchema::BOOLEAN_TYPE:
                return $defaultValue;
            case AvroSchema::INT_TYPE:
            case AvroSchema::LONG_TYPE:
                return (int) $defaultValue;
            case AvroSchema::FLOAT_TYPE:
            case AvroSchema::DOUBLE_TYPE:
                return (float) $defaultValue;
            case AvroSchema::STRING_TYPE:
            case AvroSchema::BYTES_TYPE:
                return $this->readBytes($fieldSchema, $fieldSchema, $defaultValue);
            case AvroSchema::ARRAY_SCHEMA:
                $array = [];
                foreach ($defaultValue as $jsonValue) {
                    /** @phpstan-ignore method.notFound */
                    $val = $this->readDefaultValue($fieldSchema->items(), $jsonValue);
                    $array[] = $val;
                }

                return $array;
            case AvroSchema::MAP_SCHEMA:
                $map = [];
                foreach ($defaultValue as $key => $jsonValue) {
                    $map[$key] = $this->readDefaultValue(
                        /** @phpstan-ignore method.notFound */
                        $fieldSchema->values(),
                        $jsonValue
                    );
                }

                return $map;
            case AvroSchema::UNION_SCHEMA:
                return $this->readDefaultValue(
                    /** @phpstan-ignore method.notFound */
                    $fieldSchema->schemaByIndex(0),
                    $defaultValue
                );
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::FIXED_SCHEMA:
                return $defaultValue;
            case AvroSchema::RECORD_SCHEMA:
                /** @var AvroRecordSchema $fieldSchema */
                $record = [];
                foreach ($fieldSchema->fields() as $field) {
                    $fieldName = $field->name();
                    if (!array_key_exists($fieldName, $defaultValue)) {
                        $jsonValue = $field->defaultValue();
                    } else {
                        $jsonValue = $defaultValue[$fieldName];
                    }

                    $record[$fieldName] = $this->readDefaultValue(
                        $field->type(),
                        $jsonValue
                    );
                }

                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $fieldSchema->type()));
        }
    }

    /**
     * Minimum on-wire size of a collection element schema. Exposed for the skip
     * path, which lives in the decoder.
     */
    public static function collectionElementMinBytes(AvroSchema $schema): int
    {
        return self::minBytesPerElement($schema);
    }

    /**
     * Bounds the cumulative number of elements skipped in an array or map, so a
     * huge block of zero-byte elements cannot loop unboundedly (a CPU
     * exhaustion) even though skipping allocates nothing.
     *
     * @throws AvroIOCollectionSizeException if the limit is exceeded
     */
    public static function checkSkipCollectionCount(int $existing, int $count, int $minBytes): void
    {
        if ($count <= 0) {
            return;
        }
        [$zeroByteLimit, $structuralLimit] = self::collectionLimits();
        $limit = $minBytes > 0 ? $structuralLimit : $zeroByteLimit;
        if ($count > $limit || $existing > $limit - $count) {
            throw new AvroIOCollectionSizeException($limit);
        }
    }

    private function readDecimal(string $bytes, int $scale): string
    {
        $mostSignificantBit = ord($bytes[0]) & 0x80;
        $padded = str_pad($bytes, 8, $mostSignificantBit ? "\xff" : "\x00", STR_PAD_LEFT);
        $int = unpack('J', $padded)[1];

        return (string) ($scale > 0 ? ($int / (10 ** $scale)) : $int);
    }

    /**
     * Minimum number of bytes a single value of the given schema can occupy on
     * the wire. Used to reject an array/map block count that could not be backed
     * by the bytes remaining. It returns 0 for any schema that can encode to
     * zero bytes: the null primitive, but also a record with no fields or whose
     * fields all encode to zero bytes. A zero return disables the collection
     * check for that element type (so, e.g., an array of nulls is not falsely
     * rejected). Types that cannot be resolved cheaply default to 1.
     *
     * @param array<int, bool> $visited
     */
    private static function minBytesPerElement(mixed $schema, array $visited = []): int
    {
        $type = $schema instanceof AvroSchema ? $schema->type() : $schema;
        // Named/complex field types may nest a schema object; unwrap one level.
        if ($type instanceof AvroSchema) {
            return self::minBytesPerElement($type, $visited);
        }
        if (!is_string($type)) {
            return 1;
        }
        switch ($type) {
            case AvroSchema::NULL_TYPE:
                return 0;
            case AvroSchema::FLOAT_TYPE:
                return 4;
            case AvroSchema::DOUBLE_TYPE:
                return 8;
            case AvroSchema::FIXED_SCHEMA:
                // Clamp to >= 0: a malformed fixed schema could have a negative
                // size (AvroSchema::parse does not reject it), which would make
                // this negative and cause ensureCollectionAvailable to treat the
                // element as zero-byte.
                return $schema instanceof AvroFixedSchema ? max(0, $schema->size()) : 1;
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
                if (!$schema instanceof AvroRecordSchema) {
                    return 1;
                }
                $id = spl_object_id($schema);
                if (isset($visited[$id])) {
                    return 1; // self-referencing schema: safe lower bound of 1 byte
                }
                $visited[$id] = true;
                $total = 0;
                foreach ($schema->fields() as $field) {
                    $total += self::minBytesPerElement($field->type(), $visited);
                }

                return $total;
            default:
                // boolean, int, long, bytes, string, enum, union, array, map:
                // all encode to at least one byte.
                return 1;
        }
    }

    /**
     * Returns the configured collection limits as [zeroByteLimit, structuralLimit].
     * AVRO_MAX_COLLECTION_ITEMS, when a non-negative integer, overrides both
     * limits to that value (which may raise or lower them).
     *
     * @return array{int, int}
     */
    private static function collectionLimits(): array
    {
        $value = getenv(self::MAX_COLLECTION_ITEMS_ENV);
        if (false !== $value && '' !== $value && preg_match('/^-?\d+$/', $value)) {
            $parsed = (int) $value;
            if ($parsed >= 0) {
                return [$parsed, $parsed];
            }
        }

        return [self::DEFAULT_MAX_COLLECTION_ITEMS, self::DEFAULT_MAX_COLLECTION_STRUCTURAL];
    }

    /**
     * Rejects a collection (array or map) block that could not be backed by the
     * input, before iterating.
     *
     * For elements with a positive minimum on-wire size, the declared count is
     * checked against the bytes remaining and a structural cap. For zero-byte
     * elements (e.g. an array of nulls), which consume no input and so cannot be
     * bounded by the bytes remaining, the cumulative count is checked against the
     * tighter zero-byte limit.
     *
     * @throws AvroException                 if the bytes-remaining check fails
     * @throws AvroIOCollectionSizeException if a size limit is exceeded
     */
    private static function ensureCollectionAvailable(
        AvroIOBinaryDecoder $decoder,
        int $existing,
        int $count,
        int $minBytesPerElement
    ): void {
        if ($count <= 0) {
            return;
        }
        [$zeroByteLimit, $structuralLimit] = self::collectionLimits();
        if ($minBytesPerElement > 0) {
            $remaining = $decoder->bytesRemaining();
            if ($count > intdiv($remaining, $minBytesPerElement)) {
                throw new AvroException(
                    "Collection claims {$count} elements with at least {$minBytesPerElement} "
                    ."bytes each, but only {$remaining} bytes are available."
                );
            }
            if ($count > $structuralLimit || $existing > $structuralLimit - $count) {
                throw new AvroIOCollectionSizeException($structuralLimit);
            }

            return;
        }
        if ($count > $zeroByteLimit || $existing > $zeroByteLimit - $count) {
            throw new AvroIOCollectionSizeException($zeroByteLimit);
        }
    }
}
