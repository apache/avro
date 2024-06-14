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
    public $doc;
    public $name;
    public $namespace;
    public $schemata;
    public $messages;

    /**
     * @param $json
     * @return AvroProtocol
     * @throws AvroProtocolParseException|AvroSchemaParseException
     */
    public static function parse($json)
    {
        if (is_null($json)) {
            throw new AvroProtocolParseException("Protocol can't be null");
        }

        $protocol = new AvroProtocol();
        $protocol->realParse(json_decode($json, true));
        return $protocol;
    }

    /**
     * @param $avro
     * @throws AvroProtocolParseException
     * @throws AvroSchemaParseException
     */
    public function realParse($avro)
    {
        $this->protocol = $avro["protocol"];
        $this->namespace = $avro["namespace"];
        $this->schemata = new AvroNamedSchemata();
        $this->name = $avro["protocol"];

        if (!is_null($avro["types"])) {
            $types = AvroSchema::realParse($avro["types"], $this->namespace, $this->schemata);
        }

        if (!is_null($avro["messages"])) {
            foreach ($avro["messages"] as $messageName => $messageAvro) {
                $message = new AvroProtocolMessage($messageName, $messageAvro, $this);
                $this->messages[$messageName] = $message;
            }
        }
    }

    public function md5()
    {
        return md5((string) $this, true);
    }

    /**
     * +   * @returns string the JSON-encoded representation of this Avro schema.
     * +   */
    public function __toString()
    {
        return (string) json_encode($this->toAvro());
    }

    /**
     * Internal represention of this Avro Protocol.
     * @returns mixed
     */
    public function toAvro()
    {
        $avro = array("protocol" => $this->name, "namespace" => $this->namespace);

        if (!is_null($this->doc)) {
            $avro["doc"] = $this->doc;
        }

        $avro["types"] = method_exists($this->schemata, 'toAvro') ? $this->schemata->toAvro() : [];

        $messages = array();
        foreach ($this->messages as $name => $msg) {
            $messages[$name] = $msg->toAvro();
        }
        $avro["messages"] = $messages;

        return $avro;
    }
}
