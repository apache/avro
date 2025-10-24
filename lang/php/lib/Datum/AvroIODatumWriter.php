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
use Apache\Avro\Schema\AvroLogicalType;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;

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
    public $writersSchema;

    public function __construct(?AvroSchema $writers_schema = null)
    {
        $this->writersSchema = $writers_schema;
    }

    /**
     * @param mixed $datum
     * @param AvroIOBinaryEncoder $encoder
     */
    public function write($datum, AvroIOBinaryEncoder $encoder)
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @param AvroSchema $writers_schema
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     * @return mixed
     *
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     * @throws AvroException if the type is invalid
     */
    public function writeData(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        if (!AvroSchema::isValidDatum($writers_schema, $datum)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        $this->writeValidatedData($writers_schema, $datum, $encoder);
    }

    /**
     * @param AvroSchema $writers_schema
     * @param $datum
     * @param AvroIOBinaryEncoder $encoder
     * @return void
     *
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     */
    private function writeValidatedData(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder)
    {
        switch ($writers_schema->type()) {
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
                $this->writeBytes($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::ARRAY_SCHEMA:
                $this->writeArray($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::MAP_SCHEMA:
                $this->writeMap($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::FIXED_SCHEMA:
                $this->writeFixed($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::ENUM_SCHEMA:
                $this->writeEnum($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                $this->writeRecord($writers_schema, $datum, $encoder);
                return;
            case AvroSchema::UNION_SCHEMA:
                $this->writeUnion($writers_schema, $datum, $encoder);
                return;
            default:
                throw new AvroException(sprintf(
                    'Unknown type: %s',
                    $writers_schema->type
                ));
        }
    }

    private function writeBytes(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $logicalType = $writers_schema->logicalType();
        if (
            $logicalType instanceof AvroLogicalType
            && $logicalType->name() === AvroSchema::DECIMAL_LOGICAL_TYPE
        ) {
            $scale = $logicalType->attributes()['scale'] ?? 0;
            $precision = $logicalType->attributes()['precision'] ?? null;

            $encoder->writeDecimal($datum, $scale, $precision);
            return;
        }

        $encoder->writeBytes($datum);
    }

    /**
     * @param null|boolean|int|float|string|array $datum item to be written
     * @throws AvroIOTypeException
     */
    private function writeArray(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_count = count($datum);
        if (0 < $datum_count) {
            $encoder->writeLong($datum_count);
            $items = $writers_schema->items();
            foreach ($datum as $item) {
                $this->writeValidatedData($items, $item, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    /**
     * @param $datum
     * @throws AvroIOTypeException
     */
    private function writeMap(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->writeLong($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->writeString($k);
                $this->writeValidatedData($writers_schema->values(), $v, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    private function writeFixed(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $logicalType = $writers_schema->logicalType();
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

                    if (strlen($duration) !== 12) {
                        throw new AvroException(
                            "Fixed duration size mismatch. Expected 12 bytes, got " . strlen($duration)
                        );
                    }

                    $encoder->write($duration);
                    return;
            }
        }

        $encoder->write($datum);
    }

    private function writeEnum(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_index = $writers_schema->symbolIndex($datum);
        $encoder->writeInt($datum_index);
    }

    private function writeRecord(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        foreach ($writers_schema->fields() as $field) {
            $this->writeValidatedData($field->type(), $datum[$field->name()] ?? null, $encoder);
        }
    }

    /**
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    private function writeUnion(AvroSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder): void
    {
        $datum_schema_index = -1;
        $datum_schema = null;
        foreach ($writers_schema->schemas() as $index => $schema) {
            if (AvroSchema::isValidDatum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema = $schema;
                break;
            }
        }

        if (is_null($datum_schema)) {
            throw new AvroIOTypeException($writers_schema, $datum);
        }

        $encoder->writeLong($datum_schema_index);
        $this->writeValidatedData($datum_schema, $datum, $encoder);
    }
}
