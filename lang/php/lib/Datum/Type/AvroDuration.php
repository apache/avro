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

namespace Apache\Avro\Datum\Type;

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroSchema;

class AvroDuration implements \Stringable
{
    private readonly int $months;

    private readonly int $days;

    private readonly int $milliseconds;

    /**
     * @throws AvroException
     */
    public function __construct(
        int $months,
        int $days,
        int $milliseconds
    ) {
        self::validateComponent($months, 'months');
        self::validateComponent($days, 'days');
        self::validateComponent($milliseconds, 'milliseconds');

        $this->months = $months;
        $this->days = $days;
        $this->milliseconds = $milliseconds;
    }

    public static function fromBytes(string $bytes): self
    {
        $unpackedData = unpack('Vmonths/Vdays/Vmilliseconds', $bytes);
        $months = (int) $unpackedData['months'];
        $days = (int) $unpackedData['days'];
        $milliseconds = (int) $unpackedData['milliseconds'];

        // Correct the sign for each component if it was read as a negative number
        // (i.e., if the value is 2^31 or greater, which means the sign bit was set)
        if ($months > AvroSchema::INT_MAX_VALUE) {
            $months -= AvroSchema::INT_RANGE;
        }
        if ($days > AvroSchema::INT_MAX_VALUE) {
            $days -= AvroSchema::INT_RANGE;
        }
        if ($milliseconds > AvroSchema::INT_MAX_VALUE) {
            $milliseconds -= AvroSchema::INT_RANGE;
        }

        return new self(
            $months,
            $days,
            $milliseconds
        );
    }

    public function toBytes(): string
    {
        $months = pack('V', $this->months);
        $days = pack('V', $this->days);
        $milliseconds = pack('V', $this->milliseconds);

        return $months . $days . $milliseconds;
    }

    public function __toString(): string
    {
        return $this->toBytes();
    }

    /**
     * Helper to check if a value is within the 32-bit signed range.
     * @throws AvroException If the value is out of bounds.
     */
    private static function validateComponent(int $value, string $name): void
    {
        if ($value > AvroSchema::INT_MAX_VALUE || $value < AvroSchema::INT_MIN_VALUE) {
            // Throw an appropriate exception for an out-of-bounds value
            // You may need to replace \Exception with your specific AvroException
            throw new AvroException(
                sprintf(
                    "Duration component '%s' value (%d) is out of bounds for a 32-bit signed integer.",
                    $name,
                    $value
                )
            );
        }
    }
}
