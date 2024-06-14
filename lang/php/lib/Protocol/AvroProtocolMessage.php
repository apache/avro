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

use Apache\Avro\Schema\AvroName;
use Apache\Avro\Schema\AvroNamedSchema;
use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroUnionSchema;

class AvroProtocolMessage
{
    public const SYSTEM_ERROR_TYPE = 'string';
    public $doc;
    /**
     * @var AvroRecordSchema $request
     */
    public $request;

    public $response;

    public $errors;

    private $is_one_way = false;

    /**
     * AvroProtocolMessage constructor.
     * @param $name
     * @param $avro
     * @param $protocol
     * @throws AvroProtocolParseException
     * @throws \Apache\Avro\Schema\AvroSchemaParseException
     */
    public function __construct($name, $avro, $protocol)
    {
        $this->name = $name;
        if (array_key_exists('doc', $avro)) {
            $this->doc = $avro["doc"];
        }
        $this->request = new AvroRecordSchema(
            new AvroName($name, null, $protocol->namespace),
            null,
            $avro['request'],
            $protocol->schemata,
            AvroSchema::REQUEST_SCHEMA
        );

        if (array_key_exists('response', $avro)) {
            $this->response = $protocol->schemata->schemaByName(new AvroName(
                $avro['response'],
                $protocol->namespace,
                $protocol->namespace
            ));
        } else {
            $avro["response"] = "null";
        }

        if ($this->response == null) {
            $this->response = new AvroPrimitiveSchema($avro['response']);
        }

        if (isset($avro["one-way"])) {
            $this->is_one_way = $avro["one-way"];
        } else {
            $errors = array();
            $errors[] = self::SYSTEM_ERROR_TYPE;
            if (array_key_exists('errors', $avro)) {
                if (!is_array($avro["errors"])) {
                    throw new AvroProtocolParseException("Errors must be an array");
                }

                foreach ($avro["errors"] as $error_type) {
                    $error_schema = $protocol->schemata
                        ->schemaByName(new AvroName($error_type, $protocol->namespace, $protocol->namespace));
                    if (is_null($error_schema)) {
                        throw new AvroProtocolParseException("Error type $error_type is unknown");
                    }

                    $errors[] = $error_schema->qualifiedName();
                }
            }
            $this->errors = new AvroUnionSchema($errors, $protocol->namespace, $protocol->schemata);
        }
        
        if ($this->is_one_way && $this->response->type() != AvroSchema::NULL_TYPE) {
            throw new AvroProtocolParseException("One way message $name can't have a reponse");
        }

        if ($this->is_one_way && !is_null($this->errors)) {
            throw new AvroProtocolParseException("One way message $name can't have errors");
        }
    }

    /**
     * @return array
     * @throws AvroProtocolParseException
     */
    public function toAvro()
    {
        $avro = array();
        if (!is_null($this->doc)) {
            $avro["doc"] = $this->doc;
        }

        $avro["request"] = $this->request->toAvro();

        if ($this->is_one_way()) {
            $avro["response"] = "null";
            $avro["one-way"] = true;
        } else {
            if (is_null($this->response)) {
                throw new AvroProtocolParseException(
                    "Message '" . $this->name . "' has no declared response but is not a one-way message."
                );
            }

            $response_type = $this->response->type();
            if (AvroSchema::isNamedType($response_type)) {
                $response_type = $this->response->qualifiedName();
            }

            $avro["response"] = $response_type;

            if (!is_null($this->errors)) {
                $avro["errors"] = array();

                foreach ($this->errors->schemas() as $error) {
                    $error_type = $error->type();
                    if (AvroSchema::isNamedType($error_type)) {
                        /** @var AvroNamedSchema $error_type */
                        $error_type = $error->qualifiedName();
                    }

                    $avro["errors"][] = $error_type;
                }
                array_shift($avro["errors"]);
            }
        }

        return $avro;
    }

    public function is_one_way()
    {
        return $this->is_one_way;
    }
}
