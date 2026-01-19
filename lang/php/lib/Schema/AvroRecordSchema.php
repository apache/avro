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

class AvroRecordSchema extends AvroNamedSchema
{
    /**
     * @var array<int, AvroField> array of AvroNamedSchema field definitions of
     *                   this AvroRecordSchema
     */
    private array $fields;
    /**
     * @var null|array<string, AvroField> map of field names to field objects.
     * @internal Not called directly. Memoization of AvroRecordSchema->fieldsHash()
     */
    private ?array $fieldsHash = null;

    public function __construct(
        AvroName $name,
        ?string $doc,
        ?array $fields,
        AvroNamedSchemata $schemata,
        string $schema_type = AvroSchema::RECORD_SCHEMA,
        ?array $aliases = null
    ) {
        if (is_null($fields)) {
            throw new AvroSchemaParseException(
                'Record schema requires a non-empty fields attribute'
            );
        }

        if (AvroSchema::REQUEST_SCHEMA === $schema_type) {
            parent::__construct($schema_type, $name);
        } else {
            parent::__construct($schema_type, $name, $doc, $schemata, $aliases);
        }

        [$x, $namespace] = $name->nameAndNamespace();
        $this->fields = self::parseFields($fields, $namespace, $schemata);
    }

    /**
     * @param null|string $default_namespace namespace of enclosing schema
     * @throws AvroSchemaParseException
     */
    public static function parseFields(
        array $fieldsDefinitions,
        ?string $default_namespace,
        AvroNamedSchemata $schemata
    ): array {
        $fields = [];
        $fieldNames = [];
        $aliasNames = [];
        foreach ($fieldsDefinitions as $fieldDefinition) {
            $name = $fieldDefinition[AvroField::FIELD_NAME_ATTR] ?? null;

            if (in_array($name, $fieldNames)) {
                throw new AvroSchemaParseException(
                    sprintf("Field name %s is already in use", $name)
                );
            }

            $newField = AvroField::fromFieldDefinition($fieldDefinition, $default_namespace, $schemata);

            $fieldNames[] = $name;
            if ($newField->hasAliases() && array_intersect($aliasNames, $newField->getAliases())) {
                throw new AvroSchemaParseException("Alias already in use");
            }
            if ($newField->hasAliases()) {
                array_push($aliasNames, ...$newField->getAliases());
            }
            $fields[] = $newField;
        }

        return $fields;
    }

    public function toAvro(): string|array
    {
        $avro = parent::toAvro();

        $fields_avro = [];
        foreach ($this->fields as $field) {
            $fields_avro[] = $field->toAvro();
        }

        if (AvroSchema::REQUEST_SCHEMA === $this->type) {
            return $fields_avro;
        }

        $avro[AvroSchema::FIELDS_ATTR] = $fields_avro;

        return $avro;
    }

    /**
     * @returns array the schema definitions of the fields of this AvroRecordSchema
     */
    public function fields(): array
    {
        return $this->fields;
    }

    /**
     * @return array a hash table of the fields of this AvroRecordSchema fields
     *          keyed by each field's name
     */
    public function fieldsHash(): array
    {
        if (is_null($this->fieldsHash)) {
            $hash = [];
            foreach ($this->fields as $field) {
                $hash[$field->name()] = $field;
            }
            $this->fieldsHash = $hash;
        }

        return $this->fieldsHash;
    }

    public function fieldsByAlias(): array
    {
        $hash = [];
        foreach ($this->fields as $field) {
            if ($field->hasAliases()) {
                foreach ($field->getAliases() as $a) {
                    $hash[$a] = $field;
                }
            }
        }

        return $hash;
    }
}
