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
 * @phpstan-import-type AvroAliases from AvroAliasedSchema
 */
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

    /**
     * @param null|array<string, mixed> $fields
     * @param null|AvroAliases $aliases
     * @throws AvroSchemaParseException
     */
    public function __construct(
        AvroName $name,
        ?string $doc,
        ?array $fields,
        AvroNamedSchemata $schemata,
        string $schemaType = AvroSchema::RECORD_SCHEMA,
        ?array $aliases = null
    ) {
        if (is_null($fields)) {
            throw new AvroSchemaParseException(
                'Record schema requires a non-empty fields attribute'
            );
        }

        if (AvroSchema::REQUEST_SCHEMA === $schemaType) {
            parent::__construct($schemaType, $name);
        } else {
            parent::__construct($schemaType, $name, $doc, $schemata, $aliases);
        }

        [$x, $namespace] = $name->nameAndNamespace();
        $this->fields = self::parseFields($fields, $namespace, $schemata);
    }

    /**
     * @param array<string, mixed> $fieldsDefinitions
     * @param null|string $defaultNamespace namespace of enclosing schema
     * @throws AvroSchemaParseException
     * @return array<int, AvroField>
     */
    public static function parseFields(
        array $fieldsDefinitions,
        ?string $defaultNamespace,
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

            $newField = AvroField::fromFieldDefinition($fieldDefinition, $defaultNamespace, $schemata);

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

    /**
     * @return array<string, mixed>|list<array<string, mixed>>|string the Avro representation of this AvroRecordSchema
     */
    public function toAvro(): string|array
    {
        $avro = parent::toAvro();

        $fieldsAvro = [];
        foreach ($this->fields as $field) {
            $fieldsAvro[] = $field->toAvro();
        }

        if (AvroSchema::REQUEST_SCHEMA === $this->type) {
            return $fieldsAvro;
        }

        $avro[AvroSchema::FIELDS_ATTR] = $fieldsAvro;

        return $avro;
    }

    /**
     * @return array<int, AvroField> the schema definitions of the fields of this AvroRecordSchema
     */
    public function fields(): array
    {
        return $this->fields;
    }

    /**
     * @return array<string, AvroField> a hash table of the fields of this AvroRecordSchema fields
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

    /**
     * @return array<string, AvroField>
     */
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
