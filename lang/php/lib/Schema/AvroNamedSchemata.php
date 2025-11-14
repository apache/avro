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
 *  Keeps track of AvroNamedSchema which have been observed so far,
 *  as well as the default namespace.
 */
class AvroNamedSchemata
{
    public function __construct(
        /**
         * @var AvroNamedSchema[]
         */
        private array $schemata = []
    ) {
    }

    public function listSchemas()
    {
        var_export($this->schemata);
        foreach ($this->schemata as $sch) {
            echo 'Schema '.$sch->__toString()."\n";
        }
    }

    public function schemaByName(AvroName $name): ?AvroSchema
    {
        return $this->schema($name->fullname());
    }

    /**
     * @return null|AvroSchema the schema which has the given name,
     *          or null if there is no schema with the given name.
     */
    public function schema(string $fullname): ?AvroSchema
    {
        return $this->schemata[$fullname] ?? null;
    }

    /**
     * Creates a new AvroNamedSchemata instance of this schemata instance
     * with the given $schema appended.
     * @param AvroNamedSchema $schema schema to add to this existing schemata
     * @throws AvroSchemaParseException
     */
    public function cloneWithNewSchema(AvroNamedSchema $schema): AvroNamedSchemata
    {
        $name = $schema->fullname();
        if (AvroSchema::isValidType($name)) {
            throw new AvroSchemaParseException(sprintf('Name "%s" is a reserved type name', $name));
        }
        if ($this->hasName($name)) {
            throw new AvroSchemaParseException(sprintf('Name "%s" is already in use', $name));
        }
        $schemata = new AvroNamedSchemata($this->schemata);
        $schemata->schemata[$name] = $schema;

        return $schemata;
    }

    /**
     * @returns bool true if there exists a schema with the given name
     *                  and false otherwise.
     */
    public function hasName(string $fullname): bool
    {
        return array_key_exists($fullname, $this->schemata);
    }
}
