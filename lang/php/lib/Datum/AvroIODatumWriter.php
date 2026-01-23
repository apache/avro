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
use Apache\Avro\Schema\AvroLogicalType;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;
use Apache\Avro\Schema\AvroUnionSchema;

/**
 * Handles schema-specific writing of data to the encoder.
 *
 * Ensures that each datum written is consistent with the writer's schema.
 */
class AvroIODatumWriter
{
    /**
     * Schema used by this instance to write Avro data.
     */
    public ?AvroSchema $writersSchema;

    public function __construct(?AvroSchema $writersSchema = null)
    {
        $this->writersSchema = $writersSchema;
    }

    public function write(mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     * @throws AvroException if the type is invalid
     */
    public function writeData(AvroSchema $writersSchema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        if (!AvroSchema::isValidDatum($writersSchema, $datum)) {
            throw new AvroIOTypeException($writersSchema, $datum);
        }

        $this->writeValidatedData($writersSchema, $datum, $encoder);
    }

    /**
     * @throws AvroException
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     * @throws AvroSchemaParseException
     */
    private function writeValidatedData(AvroSchema $writersSchema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        switch ($writersSchema->type()) {
            case AvroSchema::NULL_TYPE:
                $encoder->writeNull($datum);

                return;
            case AvroSchema::BOOLEAN_TYPE:
                $encoder->writeBoolean($datum);

                return;
            case AvroSchema::INT_TYPE:
                $encoder->writeInt($datum);

                return;
            case AvroSchema::LONG_TYPE:
                $encoder->writeLong($datum);

                return;
            case AvroSchema::FLOAT_TYPE:
                $encoder->writeFloat($datum);

                return;
            case AvroSchema::DOUBLE_TYPE:
                $encoder->writeDouble($datum);

                return;
            case AvroSchema::STRING_TYPE:
                $encoder->writeString($datum);

                return;
            case AvroSchema::BYTES_TYPE:
                $this->writeBytes($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::ARRAY_SCHEMA:
                $this->writeArray($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::MAP_SCHEMA:
                $this->writeMap($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::FIXED_SCHEMA:
                $this->writeFixed($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::ENUM_SCHEMA:
                $this->writeEnum($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                $this->writeRecord($writersSchema, $datum, $encoder);

                return;
            case AvroSchema::UNION_SCHEMA:
                $this->writeUnion($writersSchema, $datum, $encoder);

                return;
            default:
                throw new AvroException(sprintf(
                    'Unknown type: %s',
                    $writersSchema->type
                ));
        }
    }

    private function writeBytes(AvroSchema $writers_schema, string $datum, AvroIOBinaryEncoder $encoder): void
    {
        $logicalType = $writers_schema->logicalType();
        if (
            $logicalType instanceof AvroLogicalType
            && AvroSchema::DECIMAL_LOGICAL_TYPE === $logicalType->name()
        ) {
            $scale = $logicalType->attributes()['scale'] ?? 0;
            $precision = $logicalType->attributes()['precision'] ?? null;

            $encoder->writeDecimal($datum, $scale, $precision);

            return;
        }

        $encoder->writeBytes($datum);
    }

    /**
     * @param list<mixed> $datum
     * @throws AvroIOTypeException
     */
    private function writeArray(AvroArraySchema $writersSchema, array $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datumCount = count($datum);
        if (0 < $datumCount) {
            $encoder->writeLong($datumCount);
            $items = $writersSchema->items();
            foreach ($datum as $item) {
                $this->writeValidatedData($items, $item, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    /**
     * @param array<string, mixed> $datum
     * @throws AvroIOTypeException
     */
    private function writeMap(AvroMapSchema $writersSchema, array $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->writeLong($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->writeString($k);
                $this->writeValidatedData($writersSchema->values(), $v, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    private function writeFixed(
        AvroSchema $writersSchema,
        string|AvroDuration $datum,
        AvroIOBinaryEncoder $encoder
    ): void {
        $logicalType = $writersSchema->logicalType();
        if (
            $logicalType instanceof AvroLogicalType
        ) {
            switch ($logicalType->name()) {
                case AvroSchema::DECIMAL_LOGICAL_TYPE:
                    $scale = $logicalType->attributes()['scale'] ?? 0;
                    $precision = $logicalType->attributes()['precision'] ?? null;

                    $encoder->writeDecimal($datum, $scale, $precision);

                    return;
                case AvroSchema::DURATION_LOGICAL_TYPE:
                    if (!$datum instanceof AvroDuration) {
                        throw new AvroException(
                            "Duration datum must be an instance of AvroDuration"
                        );
                    }
                    $duration = (string) $datum;

                    if (12 !== strlen($duration)) {
                        throw new AvroException(
                            "Fixed duration size mismatch. Expected 12 bytes, got ".strlen($duration)
                        );
                    }

                    $encoder->write($duration);

                    return;
            }
        }

        $encoder->write($datum);
    }

    private function writeEnum(AvroEnumSchema $writersSchema, string $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datumIndex = $writersSchema->symbolIndex($datum);
        $encoder->writeInt($datumIndex);
    }

    private function writeRecord(AvroRecordSchema $writersSchema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        foreach ($writersSchema->fields() as $field) {
            $this->writeValidatedData($field->type(), $datum[$field->name()] ?? null, $encoder);
        }
    }

    /**
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    private function writeUnion(AvroUnionSchema $writersSchema, mixed $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_schema_index = -1;
        $datum_schema = null;
        foreach ($writersSchema->schemas() as $index => $schema) {
            if (AvroSchema::isValidDatum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema = $schema;

                break;
            }
        }

        if (is_null($datum_schema)) {
            throw new AvroIOTypeException($writersSchema, $datum);
        }

        $encoder->writeLong($datum_schema_index);
        $this->writeValidatedData($datum_schema, $datum, $encoder);
    }
}
