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
     * @var string|AvroSchema named schema name or AvroSchema
     *      of map schema values.
     */
    private $values;

    /**
     * @var boolean true if the named schema
     * XXX Couldn't we derive this based on whether or not
     * $this->values is a string?
     */
    private $is_values_schema_from_schemata;

    /**
     * @param string|AvroSchema $values
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     */
    public function __construct($values, $default_namespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::MAP_SCHEMA);

        $this->is_values_schema_from_schemata = false;
        $values_schema = null;
        if (
            is_string($values)
            && $values_schema = $schemata->schema_by_name(
                new AvroName($values, null, $default_namespace)
            )
        ) {
            $this->is_values_schema_from_schemata = true;
        } else {
            $values_schema = AvroSchema::subparse(
                $values,
                $default_namespace,
                $schemata
            );
        }

        $this->values = $values_schema;
    }

    /**
     * @returns XXX|AvroSchema
     */
    public function values()
    {
        return $this->values;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        $avro[AvroSchema::VALUES_ATTR] = $this->is_values_schema_from_schemata
            ? $this->values->qualified_name() : $this->values->to_avro();
        return $avro;
    }
}
