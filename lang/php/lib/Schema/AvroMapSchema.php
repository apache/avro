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

/**
 * Avro map schema consisting of named values of defined
 * Avro Schema types.
 * @package Avro
 */
class AvroMapSchema extends AvroSchema
{
    /**
     * AvroSchema definition based on input $values variable that could contain a primitive type or an associative
     * array with the AVRO definition.
     */
    private AvroSchema $values;

    /**
     * @var bool true if the named schema
     */
    private bool $isValuesSchemaFromSchemata;

    public function __construct(string|array $values, ?string $defaultNamespace, ?AvroNamedSchemata &$schemata = null)
    {
        parent::__construct(AvroSchema::MAP_SCHEMA);

        $this->isValuesSchemaFromSchemata = false;
        $values_schema = null;
        if (
            is_string($values)
            && $values_schema = $schemata->schemaByName(
                new AvroName($values, null, $defaultNamespace)
            )
        ) {
            $this->isValuesSchemaFromSchemata = true;
        } else {
            $values_schema = AvroSchema::subparse(
                $values,
                $defaultNamespace,
                $schemata
            );
        }

        $this->values = $values_schema;
    }

    public function values(): AvroSchema
    {
        return $this->values;
    }

    public function toAvro(): string|array
    {
        $avro = parent::toAvro();
        $avro[AvroSchema::VALUES_ATTR] = match (true) {
            $this->isValuesSchemaFromSchemata && $this->values instanceof AvroNamedSchema => $this->values->qualifiedName(),
            default => $this->values->toAvro(),
        };
        return $avro;
    }
}
