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
 */
class AvroUnionSchema extends AvroSchema
{
    /**
     * @var int[] list of indices of named schemas which
     *                are defined in $schemata
     */
    public array $schemaFromSchemataIndices;
    /**
     * @var array<int, AvroSchema> list of schemas of this union
     */
    private array $schemas;

    /**
     * @param array<int, AvroSchema|string> $schemas list of schemas in the union
     * @param null|string $defaultNamespace namespace of enclosing schema
     * @throws AvroSchemaParseException
     */
    public function __construct(array $schemas, ?string $defaultNamespace, AvroNamedSchemata $schemata)
    {
        parent::__construct(AvroSchema::UNION_SCHEMA);

        $this->schemaFromSchemataIndices = [];
        $schemaTypes = [];
        foreach ($schemas as $index => $schema) {
            $isSchemaFromSchemata = false;
            $newSchema = null;
            if (
                is_string($schema)
                && ($newSchema = $schemata->schemaByName(
                    new AvroName($schema, null, $defaultNamespace)
                ))
            ) {
                $isSchemaFromSchemata = true;
            } else {
                $newSchema = self::subparse($schema, $defaultNamespace, $schemata);
            }

            $schemaType = $newSchema->type;
            if (
                self::isValidType($schemaType)
                && !self::isNamedType($schemaType)
                && in_array($schemaType, $schemaTypes)
            ) {
                throw new AvroSchemaParseException(sprintf('"%s" is already in union', $schemaType));
            }

            if (AvroSchema::UNION_SCHEMA === $schemaType) {
                throw new AvroSchemaParseException('Unions cannot contain other unions');
            }

            $schemaTypes[] = $schemaType;
            $this->schemas[] = $newSchema;
            if ($isSchemaFromSchemata) {
                $this->schemaFromSchemataIndices[] = $index;
            }
        }
    }

    /**
     * @return array<int, AvroSchema>
     */
    public function schemas(): array
    {
        return $this->schemas;
    }

    /**
     * @param mixed $index
     * @throws AvroSchemaParseException if the index is invalid for this schema.
     * @return AvroSchema the particular schema from the union for
     * the given (zero-based) index.
     */
    public function schemaByIndex($index): AvroSchema
    {
        if (count($this->schemas) > $index) {
            return $this->schemas[$index];
        }

        throw new AvroSchemaParseException('Invalid union schema index');
    }

    /**
     * @return array<int, mixed>|string Avro representation of this schema
     */
    public function toAvro(): string|array
    {
        $avro = [];

        foreach ($this->schemas as $index => $schema) {
            $avro[] = in_array($index, $this->schemaFromSchemataIndices) && $schema instanceof AvroNamedSchema
                ? $schema->qualifiedName()
                : $schema->toAvro();
        }

        return $avro;
    }
}
