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

namespace Apache\Avro\Datum;

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroArraySchema;
use Apache\Avro\Schema\AvroEnumSchema;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroUnionSchema;

/**
 * Writes data from code-generated (specific) record classes to the encoder.
 *
 * It knows how to extract field values from generated PHP class instances (via getter methods) and backed enum
 * instances, encoding them according to the Avro schema.
 *
 * Generated records expose each field through a getter method whose name matches the Avro field name.
 * Generated enums are PHP 8.1+ backed string enums whose ->value holds the Avro symbol string.
 *
 * Usage:
 *     $schema  = AvroSchema::parse($json);
 *     $writer  = new AvroSpecificDatumWriter($schema);
 *     $io      = new \Apache\Avro\IO\AvroStringIO();
 *     $encoder = new AvroIOBinaryEncoder($io);
 *     $writer->write($myGeneratedObject, $encoder);
 *     $bytes   = $io->string();
 */
class AvroSpecificDatumWriter
{
    public function __construct(
        private readonly AvroSchema $writersSchema
    ) {
    }

    /**
     * Serializes the given datum (a generated record instance) to the encoder.
     *
     * @throws AvroException
     */
    public function write(object $datum, AvroIOBinaryEncoder $encoder): void
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @throws AvroException
     */
    private function writeData(AvroSchema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        match (true) {
            $schema instanceof AvroRecordSchema => $this->writeRecord($schema, $datum, $encoder),
            $schema instanceof AvroEnumSchema => $this->writeEnum($schema, $datum, $encoder),
            $schema instanceof AvroArraySchema => $this->writeArray($schema, $datum, $encoder),
            $schema instanceof AvroMapSchema => $this->writeMap($schema, $datum, $encoder),
            $schema instanceof AvroUnionSchema => $this->writeUnion($schema, $datum, $encoder),
            $schema instanceof AvroPrimitiveSchema => $this->writePrimitive($schema, $datum, $encoder),
            default => throw new AvroException(sprintf('Unsupported schema type: %s', $schema->type())),
        };
    }

    /**
     * Writes a record by calling the getter for each field defined in the schema.
     *
     * @throws AvroException
     */
    private function writeRecord(AvroRecordSchema $schema, object $datum, AvroIOBinaryEncoder $encoder): void
    {
        foreach ($schema->fields() as $field) {
            $getter = $field->name();
            if (!method_exists($datum, $getter)) {
                throw new AvroIOTypeException($schema, $datum);
            }
            $value = $datum->{$getter}();
            $this->writeData($field->type(), $value, $encoder);
        }
    }

    /**
     * Writes a backed enum value by looking up its symbol index.
     *
     * @throws AvroException
     */
    private function writeEnum(AvroEnumSchema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        if (!$datum instanceof \BackedEnum) {
            throw new AvroIOTypeException($schema, $datum);
        }

        $symbolIndex = $schema->symbolIndex($datum->value);
        $encoder->writeInt($symbolIndex);
    }

    /**
     * @param list<mixed> $datum
     *
     * @throws AvroException
     */
    private function writeArray(AvroArraySchema $schema, array $datum, AvroIOBinaryEncoder $encoder): void
    {
        $count = count($datum);
        if ($count > 0) {
            $encoder->writeLong($count);
            foreach ($datum as $item) {
                $this->writeData($schema->items(), $item, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    /**
     * @param array<string, mixed> $datum
     *
     * @throws AvroException
     */
    private function writeMap(AvroMapSchema $schema, array $datum, AvroIOBinaryEncoder $encoder): void
    {
        $count = count($datum);
        if ($count > 0) {
            $encoder->writeLong($count);
            foreach ($datum as $key => $value) {
                $encoder->writeString((string) $key);
                $this->writeData($schema->values(), $value, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    /**
     * Writes a union value by finding the matching branch schema.
     *
     * @throws AvroIOTypeException if no branch matches the datum
     * @throws AvroException
     */
    private function writeUnion(AvroUnionSchema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        $matchedIndex = null;
        $matchedSchema = null;

        foreach ($schema->schemas() as $index => $branchSchema) {
            if ($this->datumMatchesSchema($branchSchema, $datum)) {
                $matchedIndex = $index;
                $matchedSchema = $branchSchema;

                break;
            }
        }

        if (null === $matchedSchema) {
            throw new AvroIOTypeException($schema, $datum);
        }

        $encoder->writeLong($matchedIndex);
        $this->writeData($matchedSchema, $datum, $encoder);
    }

    /**
     * Writes a primitive value using the appropriate encoder method.
     *
     * @throws AvroException
     */
    private function writePrimitive(AvroPrimitiveSchema $schema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        match ($schema->type()) {
            AvroSchema::NULL_TYPE => $encoder->writeNull($datum),
            AvroSchema::BOOLEAN_TYPE => $encoder->writeBoolean($datum),
            AvroSchema::INT_TYPE => $encoder->writeInt($datum),
            AvroSchema::LONG_TYPE => $encoder->writeLong($datum),
            AvroSchema::FLOAT_TYPE => $encoder->writeFloat($datum),
            AvroSchema::DOUBLE_TYPE => $encoder->writeDouble($datum),
            AvroSchema::STRING_TYPE => $encoder->writeString($datum),
            AvroSchema::BYTES_TYPE => $encoder->writeBytes($datum),
            default => throw new AvroException(sprintf('Unknown primitive type: %s', $schema->type())),
        };
    }

    /**
     * Determines whether the given datum matches the given schema branch.
     * Used by writeUnion() to find the correct branch index.
     */
    private function datumMatchesSchema(AvroSchema $schema, mixed $datum): bool
    {
        return match (true) {
            $schema instanceof AvroPrimitiveSchema => $this->datumMatchesPrimitive($schema, $datum),
            $schema instanceof AvroEnumSchema => $datum instanceof \BackedEnum
                && $this->classNameMatchesSchema($datum, $schema->name()),
            $schema instanceof AvroRecordSchema => is_object($datum)
                && !($datum instanceof \BackedEnum)
                && $this->classNameMatchesSchema($datum, $schema->name()),
            $schema instanceof AvroArraySchema => is_array($datum)
                && ([] === $datum || array_is_list($datum)),
            $schema instanceof AvroMapSchema => is_array($datum),
            default => false,
        };
    }

    private function datumMatchesPrimitive(AvroPrimitiveSchema $schema, mixed $datum): bool
    {
        return match ($schema->type()) {
            AvroSchema::NULL_TYPE => null === $datum,
            AvroSchema::BOOLEAN_TYPE => is_bool($datum),
            AvroSchema::INT_TYPE => is_int($datum)
                && $datum >= AvroSchema::INT_MIN_VALUE
                && $datum <= AvroSchema::INT_MAX_VALUE,
            AvroSchema::LONG_TYPE => is_int($datum),
            AvroSchema::FLOAT_TYPE, AvroSchema::DOUBLE_TYPE => is_float($datum) || is_int($datum),
            AvroSchema::STRING_TYPE, AvroSchema::BYTES_TYPE => is_string($datum),
            default => false,
        };
    }

    /**
     * Checks whether the short class name of the datum matches the Avro schema name.
     * Generated classes use ucwords(schemaName) as the class name.
     */
    private function classNameMatchesSchema(object $datum, string $schemaName): bool
    {
        $className = (new \ReflectionClass($datum))->getShortName();

        return 0 === strcasecmp($className, $schemaName);
    }
}
