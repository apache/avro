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

/**
 * AvroNamedSchema with fixed-length data values
 * @package Avro
 */
class AvroFixedSchema extends AvroNamedSchema
{
    /**
     * @var int byte count of this fixed schema data value
     */
    private $size;

    /**
     * @param AvroName $name
     * @param string $doc Set to null, as fixed schemas don't have doc strings
     * @param int $size byte count of this fixed schema data value
     * @param AvroNamedSchemata &$schemata
     * @param array $aliases
     * @throws AvroSchemaParseException
     */
    public function __construct($name, $doc, $size, &$schemata = null, $aliases = null)
    {
        if (!is_int($size)) {
            throw new AvroSchemaParseException(
                'Fixed Schema requires a valid integer for "size" attribute'
            );
        }
        parent::__construct(AvroSchema::FIXED_SCHEMA, $name, null, $schemata, $aliases);
        $this->size = $size;
    }

    /**
     * @returns int byte count of this fixed schema data value
     */
    public function size()
    {
        return $this->size;
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();
        $avro[AvroSchema::SIZE_ATTR] = $this->size;
        return $avro;
    }

    /**
     * @param array<int, string>|null $aliases
     * @throws AvroSchemaParseException
     */
    public static function duration(
        AvroName $name,
        ?string $doc,
        AvroNamedSchemata &$schemata = null,
        ?array $aliases = null
    ): self {
        $fixedSchema = new self($name, $doc, 12, $schemata, $aliases);

        $fixedSchema->logicalType = AvroLogicalType::duration();

        return $fixedSchema;
    }

    /**
     * @param array<int, string>|null $aliases
     * @throws AvroSchemaParseException
     * @throws AvroException
     */
    public static function decimal(
        AvroName $name,
        ?string $doc,
        int $size,
        int $precision,
        int $scale,
        ?AvroNamedSchemata &$schemata = null,
        ?array $aliases = null
    ): self {
        $self = new self($name, $doc, $size, $schemata, $aliases);

        $maxPrecision = (int) floor(log10(self::maxDecimalMagnitude($size)));

        if ($precision > $maxPrecision) {
            throw new AvroException(
                "Invalid precision for specified fixed size (size='{$size}', precision='{$precision}')."
            );
        }

        $self->logicalType = AvroLogicalType::decimal($precision, $scale);

        return $self;
    }

    public static function maxDecimalMagnitude(int $size): float
    {
        return (float) (2 ** ((8 * $size) - 1)) - 1;
    }
}
