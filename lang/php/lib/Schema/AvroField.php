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
 * Field of an {@link AvroRecordSchema}
 * @package Avro
 */
class AvroField extends AvroSchema
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
     * @var array list of valid field sort order values
     */
    private static array $validFieldSortOrders = [
        self::ASC_SORT_ORDER,
        self::DESC_SORT_ORDER,
        self::IGNORE_SORT_ORDER
    ];

    private ?string $name;

    private bool $isTypeFromSchemata;

    /**
     * @var bool whether or no there is a default value
     */
    private bool $hasDefault;

    /**
     * @var string field default value
     */
    private mixed $default;
    /**
     * @var null|string sort order of this field
     */
    private ?string $order;
    /**
     * @var array|null
     */
    private ?array $aliases;

    /**
     * @throws AvroSchemaParseException
     * @todo Check validity of $default value
     */
    public function __construct(
        ?string $name,
        string|AvroSchema $schema,
        bool $isTypeFromSchemata,
        bool $hasDefault,
        mixed $default,
        ?string $order = null,
        mixed $aliases = null
    ) {
        if (!AvroName::isWellFormedName($name)) {
            throw new AvroSchemaParseException('Field requires a "name" attribute');
        }

        parent::__construct($schema);
        $this->name = $name;
        $this->isTypeFromSchemata = $isTypeFromSchemata;
        $this->hasDefault = $hasDefault;
        if ($this->hasDefault) {
            $this->default = $default;
        }
        self::checkOrderValue($order);
        $this->order = $order;
        self::hasValidAliases($aliases);
        $this->aliases = $aliases;
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

        return $avro;
    }

    /**
     * @returns string the name of this field
     */
    public function name()
    {
        return $this->name;
    }

    /**
     * @returns mixed the default value of this field
     */
    public function defaultValue()
    {
        return $this->default;
    }

    /**
     * @returns boolean true if the field has a default and false otherwise
     */
    public function hasDefaultValue()
    {
        return $this->hasDefault;
    }

    public function getAliases()
    {
        return $this->aliases;
    }

    public function hasAliases()
    {
        return $this->aliases !== null;
    }
}
