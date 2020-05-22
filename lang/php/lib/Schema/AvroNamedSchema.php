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
 * @package Avro
 * @todo Refactor AvroNamedSchema to use an AvroName instance
 *       to store name information.
 */
class AvroNamedSchema extends AvroSchema
{
    /**
     * @var AvroName $name
     */
    private $name;

    /**
     * @var string documentation string
     */
    private $doc;

    /**
     * @param string $type
     * @param AvroName $name
     * @param string $doc documentation string
     * @param AvroNamedSchemata &$schemata
     * @throws AvroSchemaParseException
     */
    public function __construct($type, $name, $doc = null, &$schemata = null)
    {
        parent::__construct($type);
        $this->name = $name;

        if ($doc && !is_string($doc)) {
            throw new AvroSchemaParseException('Schema doc attribute must be a string');
        }
        $this->doc = $doc;

        if (!is_null($schemata)) {
            $schemata = $schemata->clone_with_new_schema($this);
        }
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();
        [$name, $namespace] = AvroName::extract_namespace($this->qualified_name());
        $avro[AvroSchema::NAME_ATTR] = $name;
        if ($namespace) {
            $avro[AvroSchema::NAMESPACE_ATTR] = $namespace;
        }
        if (!is_null($this->doc)) {
            $avro[AvroSchema::DOC_ATTR] = $this->doc;
        }
        return $avro;
    }

    public function qualified_name()
    {
        return $this->name->qualified_name();
    }

    /**
     * @returns string
     */
    public function fullname()
    {
        return $this->name->fullname();
    }
}
