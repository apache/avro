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

namespace Apache\Avro\Protocol;

use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroNamedSchemata;
use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;

class AvroProtocolMessage
{
    public readonly AvroRecordSchema $request;

    public readonly ?AvroSchema $response;

    /**
     * @throws AvroSchemaParseException
     */
    public function __construct(
        public string $name,
        array $avro,
        string $namespace,
        AvroNamedSchemata $schemata,
    ) {
        $this->request = new AvroRecordSchema(
            name: new AvroName($this->name, null, $namespace),
            doc: null,
            fields: $avro['request'],
            schemata: $schemata,
            schema_type: AvroSchema::REQUEST_SCHEMA
        );

        $response = null;
        if (array_key_exists('response', $avro)) {
            $response = $schemata->schemaByName(
                new AvroName(
                    name: $avro['response'],
                    namespace: $namespace,
                    default_namespace: $namespace
                )
            );

            if (is_null($response)) {
                $response = new AvroPrimitiveSchema($avro['response']);
            }
        }
        $this->response = $response;
    }
}
