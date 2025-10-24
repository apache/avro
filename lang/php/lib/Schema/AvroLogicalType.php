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

namespace Apache\Avro\Schema;

use Apache\Avro\AvroException;

class AvroLogicalType
{
    public const ATTRIBUTE_DECIMAL_PRECISION = 'precision';
    public const ATTRIBUTE_DECIMAL_SCALE = 'scale';

    public function __construct(private readonly string $name, private readonly array $attributes = [])
    {
    }

    public function name(): string
    {
        return $this->name;
    }

    public function attributes(): array
    {
        return $this->attributes;
    }

    public function toAvro(): array
    {
        $avro[AvroSchema::LOGICAL_TYPE_ATTR] = $this->name;
        return array_merge($avro, $this->attributes);
    }

    public static function decimal(int $precision, int $scale): self
    {
        if ($precision <= 0) {
            throw new AvroException("Precision '{$precision}' is invalid. It must be a positive integer.");
        }

        if ($scale < 0) {
            throw new AvroException("Scale '{$scale}' is invalid. It must be a non-negative integer.");
        }

        if ($scale >= $precision) {
            throw new AvroException(
                "Scale must be a lower than precision (scale='{$scale}', precision='{$precision}')."
            );
        }

        return new AvroLogicalType(
            AvroSchema::DECIMAL_LOGICAL_TYPE,
            [
                self::ATTRIBUTE_DECIMAL_PRECISION => $precision,
                self::ATTRIBUTE_DECIMAL_SCALE => $scale,

            ]
        );
    }

    public static function uuid(): self
    {
        return new AvroLogicalType(AvroSchema::UUID_LOGICAL_TYPE);
    }

    public static function date(): self
    {
        return new AvroLogicalType(AvroSchema::DATE_LOGICAL_TYPE);
    }

    public static function timeMillis(): self
    {
        return new AvroLogicalType(AvroSchema::TIME_MILLIS_LOGICAL_TYPE);
    }

    public static function timeMicros(): self
    {
        return new AvroLogicalType(AvroSchema::TIME_MICROS_LOGICAL_TYPE);
    }

    public static function timestampMillis(): self
    {
        return new AvroLogicalType(AvroSchema::TIMESTAMP_MILLIS_LOGICAL_TYPE);
    }

    public static function timestampMicros(): self
    {
        return new AvroLogicalType(AvroSchema::TIMESTAMP_MICROS_LOGICAL_TYPE);
    }

    public static function localTimestampMillis(): self
    {
        return new AvroLogicalType(AvroSchema::LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE);
    }

    public static function localTimestampMicros(): self
    {
        return new AvroLogicalType(AvroSchema::LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE);
    }

    public static function duration(): self
    {
        return new AvroLogicalType(AvroSchema::DURATION_LOGICAL_TYPE);
    }
}
