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
 * Avro array schema, consisting of items of a particular
 * Avro schema type.
 */
class AvroArraySchema extends AvroSchema
{
    /**
     * @var AvroSchema The schema of the array elements
     */
    private AvroSchema $items;

    private bool $isItemsSchemaFromSchemata;

    /**
     * @param mixed|string $items AvroNamedSchema name or object form
     *        of decoded JSON schema representation.
     * @throws AvroSchemaParseException
     */
    public function __construct($items, ?string $defaultNamespace, AvroNamedSchemata $schemata)
    {
        parent::__construct(AvroSchema::ARRAY_SCHEMA);

        $itemsSchema = null;
        $this->isItemsSchemaFromSchemata = false;
        if (
            is_string($items)
            && $itemsSchema = $schemata->schemaByName(
                new AvroName($items, null, $defaultNamespace)
            )
        ) {
            $this->isItemsSchemaFromSchemata = true;
        } else {
            $itemsSchema = AvroSchema::subparse($items, $defaultNamespace, $schemata);
        }

        $this->items = $itemsSchema;
    }

    /**
     * @return AvroName|AvroSchema named schema name or AvroSchema
     *          of this array schema's elements.
     */
    public function items(): AvroName|AvroSchema
    {
        return $this->items;
    }

    public function toAvro(): string|array
    {
        $avro = parent::toAvro();
        $avro[AvroSchema::ITEMS_ATTR] = $this->isItemsSchemaFromSchemata && $this->items instanceof AvroNamedSchema
            ? $this->items->qualifiedName() : $this->items->toAvro();

        return $avro;
    }
}
