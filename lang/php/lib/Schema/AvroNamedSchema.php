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
 * Parent class of named Avro schema
 */
class AvroNamedSchema extends AvroSchema implements AvroAliasedSchema
{
    /**
     * @throws AvroSchemaParseException
     */
    public function __construct(
        string $type,
        private readonly AvroName $name,
        private readonly ?string $doc = null,
        ?AvroNamedSchemata &$schemata = null,
        private ?array $aliases = null
    ) {
        parent::__construct($type);

        if ($this->aliases) {
            self::hasValidAliases($this->aliases);
        }

        if (!is_null($schemata)) {
            $schemata = $schemata->cloneWithNewSchema($this);
        }
    }

    public function getAliases(): ?array
    {
        return $this->aliases;
    }

    public function toAvro(): string|array
    {
        $avro = parent::toAvro();
        [$name, $namespace] = AvroName::extractNamespace($this->qualifiedName());
        $avro[AvroSchema::NAME_ATTR] = $name;
        if ($namespace) {
            $avro[AvroSchema::NAMESPACE_ATTR] = $namespace;
        }
        if (!is_null($this->doc)) {
            $avro[AvroSchema::DOC_ATTR] = $this->doc;
        }
        if (!is_null($this->aliases)) {
            $avro[AvroSchema::ALIASES_ATTR] = $this->aliases;
        }

        return $avro;
    }

    public function qualifiedName(): string
    {
        return $this->name->qualifiedName();
    }

    public function fullname(): string
    {
        return $this->name->fullname();
    }

    public function namespace(): ?string
    {
        return $this->name->namespace();
    }
}
