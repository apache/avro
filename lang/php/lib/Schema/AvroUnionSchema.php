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
 * Union of Avro schemas, of which values can be of any of the schema in
 * the union.
 * @package Avro
 */
class AvroUnionSchema extends AvroSchema
{
    /**
     * @var int[] list of indices of named schemas which
     *                are defined in $schemata
     */
    public $schema_from_schemata_indices;
    /**
     * @var AvroSchema[] list of schemas of this union
     */
    private $schemas;

    /**
     * @param AvroSchema[] $schemas list of schemas in the union
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     */
    public function __construct($schemas, $default_namespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::UNION_SCHEMA);

        $this->schema_from_schemata_indices = array();
        $schema_types = array();
        foreach ($schemas as $index => $schema) {
            $is_schema_from_schemata = false;
            $new_schema = null;
            if (is_string($schema)
                && ($new_schema = $schemata->schema_by_name(
                    new AvroName($schema, null, $default_namespace)))) {
                $is_schema_from_schemata = true;
            } else {
                $new_schema = self::subparse($schema, $default_namespace, $schemata);
            }

            $schema_type = $new_schema->type;
            if (self::is_valid_type($schema_type)
                && !self::is_named_type($schema_type)
                && in_array($schema_type, $schema_types)) {
                throw new AvroSchemaParseException(
                    sprintf('"%s" is already in union', $schema_type));
            } elseif (AvroSchema::UNION_SCHEMA == $schema_type) {
                throw new AvroSchemaParseException('Unions cannot contain other unions');
            } else {
                $schema_types [] = $schema_type;
                $this->schemas [] = $new_schema;
                if ($is_schema_from_schemata) {
                    $this->schema_from_schemata_indices [] = $index;
                }
            }
        }

    }

    /**
     * @returns AvroSchema[]
     */
    public function schemas()
    {
        return $this->schemas;
    }

    /**
     * @returns AvroSchema the particular schema from the union for
     * the given (zero-based) index.
     * @throws AvroSchemaParseException if the index is invalid for this schema.
     */
    public function schema_by_index($index)
    {
        if (count($this->schemas) > $index) {
            return $this->schemas[$index];
        }

        throw new AvroSchemaParseException('Invalid union schema index');
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = array();

        foreach ($this->schemas as $index => $schema) {
            $avro [] = (in_array($index, $this->schema_from_schemata_indices))
                ? $schema->qualified_name() : $schema->to_avro();
        }

        return $avro;
    }
}
