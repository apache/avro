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

use Apache\Avro\Schema\AvroNamedSchemata;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;

/**
 * Avro library for protocols
 * @package Avro
 */
class AvroProtocol
{
    public function __construct(
        public readonly string $protocol,
        public readonly string $name,
        public readonly string $namespace,
        public readonly AvroNamedSchemata $schemata,
        /** @var array<int, AvroProtocolMessage> */
        public readonly array $messages,
    ) {
    }

    /**
     * @throws AvroProtocolParseException
     * @throws AvroSchemaParseException
     */
    public static function parse(string $json): self
    {
        if (false === json_validate($json)) {
            throw new AvroProtocolParseException("Protocol can't be null");
        }

        return self::realParse(json_decode($json, true, JSON_THROW_ON_ERROR));
    }

    /**
     * @param array $avro AVRO protocol as associative array
     * @throws AvroSchemaParseException
     */
    public static function realParse(array $avro): self
    {
        $schemata = new AvroNamedSchemata();

        if (!is_null($avro["types"])) {
            AvroSchema::realParse($avro["types"], $avro["namespace"], $schemata);
        }

        $messages = [];
        if (!is_null($avro["messages"])) {
            foreach ($avro["messages"] as $messageName => $messageAvro) {
                $messages[] = new AvroProtocolMessage(
                    name: $messageName,
                    avro: $messageAvro,
                    namespace: $avro["namespace"],
                    schemata: $schemata
                );
            }
        }

        return new self(
            protocol: $avro["protocol"],
            name: $avro["protocol"],
            namespace: $avro["namespace"],
            schemata: $schemata,
            messages: $messages
        );
    }
}
