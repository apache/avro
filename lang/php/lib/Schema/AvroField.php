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

namespace Apache\Avro\Schema;

/**
 * Field of an {@link AvroRecordSchema}
 *
 * @phpstan-import-type AvroSchemaDefinitionArray from AvroSchema
 * @phpstan-import-type AvroAliases from AvroAliasedSchema
 */
class AvroField extends AvroSchema implements AvroAliasedSchema
{
    /**
     * @var string fields name attribute name
     */
    public const FIELD_NAME_ATTR = 'name';

    /**
     * @var string
     */
    public const DEFAULT_ATTR = 'default';

    /**
     * @var string
     */
    public const ORDER_ATTR = 'order';

    /**
     * @var string
     */
    public const ASC_SORT_ORDER = 'ascending';

    /**
     * @var string
     */
    public const DESC_SORT_ORDER = 'descending';

    /**
     * @var string
     */
    public const IGNORE_SORT_ORDER = 'ignore';

    /**
     * @var list<string> list of valid field sort order values
     */
    private static array $validFieldSortOrders = [
        self::ASC_SORT_ORDER,
        self::DESC_SORT_ORDER,
        self::IGNORE_SORT_ORDER,
    ];

    private string $name;

    private bool $isTypeFromSchemata;

    /**
     * @var bool whether or no there is a default value
     */
    private bool $hasDefault;

    /**
     * @var mixed field default value
     */
    private mixed $default;
    /**
     * @var null|string sort order of this field
     */
    private ?string $order;

    /** @var null|AvroAliases */
    private ?array $aliases;
    private ?string $doc;

    /**
     * @param array<string> $aliases
     * @todo Check validity of $default value
     */
    private function __construct(
        string $name,
        string|AvroSchema $schema,
        bool $isTypeFromSchemata,
        bool $hasDefault,
        mixed $default,
        ?string $order = null,
        ?array $aliases = null,
        ?string $doc = null
    ) {
        parent::__construct($schema);
        $this->name = $name;
        $this->isTypeFromSchemata = $isTypeFromSchemata;
        $this->hasDefault = $hasDefault;
        if ($this->hasDefault) {
            $this->default = $default;
        }
        $this->order = $order;
        $this->aliases = $aliases;

        $this->doc = $doc;
    }

    /**
     * @param AvroSchemaDefinitionArray $avro
     * @throws AvroSchemaParseException
     */
    public static function fromFieldDefinition(array $avro, ?string $defaultNamespace, AvroNamedSchemata $schemata): self
    {
        $name = $avro[self::FIELD_NAME_ATTR] ?? null;
        $type = $avro[AvroSchema::TYPE_ATTR] ?? null;
        $order = $avro[self::ORDER_ATTR] ?? null;
        $aliases = $avro[AvroSchema::ALIASES_ATTR] ?? null;
        $doc = $avro[AvroSchema::DOC_ATTR] ?? null;

        if (!AvroName::isWellFormedName($name)) {
            throw new AvroSchemaParseException('Field requires a "name" attribute');
        }

        self::checkOrderValue($order);
        self::hasValidAliases($aliases);
        self::hasValidDoc($doc);

        $default = null;
        $hasDefault = false;
        if (array_key_exists(self::DEFAULT_ATTR, $avro)) {
            $default = $avro[self::DEFAULT_ATTR];
            $hasDefault = true;
        }

        $isSchemaFromSchemata = false;
        $fieldAvroSchema = null;
        if (
            is_string($type)
            && $fieldAvroSchema = $schemata->schemaByName(
                new AvroName($type, null, $defaultNamespace)
            )
        ) {
            $isSchemaFromSchemata = true;
        } elseif (is_string($type) && self::isPrimitiveType($type)) {
            $fieldAvroSchema = self::subparse($avro, $defaultNamespace, $schemata);
        } else {
            $fieldAvroSchema = self::subparse($type, $defaultNamespace, $schemata);
        }

        return new self(
            name: $name,
            schema: $fieldAvroSchema,
            isTypeFromSchemata: $isSchemaFromSchemata,
            hasDefault: $hasDefault,
            default: $default,
            order: $order,
            aliases: $aliases,
            doc: $doc
        );
    }

    /**
     * @return AvroSchemaDefinitionArray|string the Avro representation of this field
     */
    public function toAvro(): string|array
    {
        $avro = [self::FIELD_NAME_ATTR => $this->name];

        $avro[AvroSchema::TYPE_ATTR] = match (true) {
            $this->isTypeFromSchemata && $this->type instanceof AvroNamedSchema => $this->type->qualifiedName(),
            $this->type instanceof AvroSchema => $this->type->toAvro(),
            is_string($this->type) => $this->type,
        };

        if (isset($this->default)) {
            $avro[self::DEFAULT_ATTR] = $this->default;
        }

        if ($this->order) {
            $avro[self::ORDER_ATTR] = $this->order;
        }

        if (!is_null($this->aliases)) {
            $avro[AvroSchema::ALIASES_ATTR] = $this->aliases;
        }

        if (!is_null($this->doc)) {
            $avro[AvroSchema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }

    /**
     * @return string the name of this field
     */
    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return mixed the default value of this field
     */
    public function defaultValue(): mixed
    {
        return $this->default;
    }

    /**
     * @return bool true if the field has a default and false otherwise
     */
    public function hasDefaultValue(): bool
    {
        return $this->hasDefault;
    }

    /**
     * @return null|AvroAliases
     */
    public function getAliases(): ?array
    {
        return $this->aliases;
    }

    public function hasAliases(): bool
    {
        return null !== $this->aliases;
    }

    public function getDoc(): ?string
    {
        return $this->doc;
    }

    public function hasDoc(): bool
    {
        return null !== $this->doc;
    }

    /**
     * @throws AvroSchemaParseException if $order is not a valid
     *                                  field order value.
     */
    private static function checkOrderValue(?string $order): void
    {
        if (!is_null($order) && !self::isValidFieldSortOrder($order)) {
            throw new AvroSchemaParseException(
                sprintf('Invalid field sort order %s', $order)
            );
        }
    }

    private static function isValidFieldSortOrder(string $order): bool
    {
        return in_array($order, self::$validFieldSortOrders, true);
    }
}
