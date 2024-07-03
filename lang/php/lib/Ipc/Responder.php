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

namespace Apache\Avro\Ipc;

use Apache\Avro\AvroException;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Protocol\AvroProtocol;
use Apache\Avro\Protocol\AvroProtocolMessage;
use Apache\Avro\Protocol\AvroProtocolParseException;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;

/**
 * Base class for the client side of a protocol interaction.
 * @package Avro
 */
abstract class Responder
{
    public const HANDSHAKE_REQUEST_SCHEMA_JSON = <<<HRSJ
    {
      "type": "record",
      "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
      "fields": [
        {"name": "clientHash",
         "type": {"type": "fixed", "name": "MD5", "size": 16}},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    HRSJ;

    public const HANDSHAKE_RESPONSE_SCHEMA_JSON = <<<HRSJ
    {
      "type": "record",
      "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
      "fields": [
        {"name": "match",
         "type": {"type": "enum", "name": "HandshakeMatch",
                  "symbols": ["BOTH", "CLIENT", "NONE"]}},
        {"name": "serverProtocol",
         "type": ["null", "string"]},
        {"name": "serverHash",
         "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
        {"name": "meta",
         "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    HRSJ;

    protected $local_protocol;
    protected $local_hash;
    protected $protocol_cache = array();

    protected $handshake_responder_writer;
    protected $handshake_responder_reader;
    protected $meta_writer;
    protected $meta_reader;

    protected $system_error_schema;

    public function __construct(AvroProtocol $local_protocol)
    {
        $this->local_protocol = $local_protocol;
        $this->local_hash = $local_protocol->md5();
        $this->protocol_cache[$this->local_hash] = $this->local_protocol;

        $this->handshake_responder_writer = new AvroIODatumWriter(
            AvroSchema::parse(Responder::HANDSHAKE_RESPONSE_SCHEMA_JSON)
        );
        $this->handshake_responder_reader = new AvroIODatumReader(
            AvroSchema::parse(Responder::HANDSHAKE_REQUEST_SCHEMA_JSON)
        );
        $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
        $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));

        $this->system_error_schema = AvroSchema::parse('["string"]');
    }

    public function local_protocol()
    {
        return $this->local_protocol;
    }

    /**
     * Entry point to process one procedure call.
     * @param string $call_request the serialized procedure call request
     * @param Transceiver $transceiver the transceiver used for the response
     * @return string|null the serialiazed procedure call response or null if it's a one-way message
     * @throws AvroException
     */
    public function respond($call_request, Transceiver $transceiver)
    {
        $buffer_reader = new AvroStringIO($call_request);
        $decoder = new AvroIOBinaryDecoder($buffer_reader);

        $buffer_writer = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($buffer_writer);

        $error = null;
        $response_metadata = [];
        try {
            $remote_protocol = $this->process_handshake($decoder, $encoder, $transceiver);
            if (is_null($remote_protocol)) {
                return $buffer_writer->string();
            }

            $request_metadata = $this->meta_reader->read($decoder);
            $remote_message_name = $decoder->readString();
            if (!isset($remote_protocol->messages[$remote_message_name])) {
                throw new AvroException("Unknown remote message: $remote_message_name");
            }
            $remote_message = $remote_protocol->messages[$remote_message_name];

            if (!isset($this->local_protocol->messages[$remote_message_name])) {
                throw new AvroException("Unknown local message: $remote_message_name");
            }
            $local_message = $this->local_protocol->messages[$remote_message_name];

            $datum_reader = new AvroIODatumReader($remote_message->request, $local_message->request);
            $request = $datum_reader->read($decoder);
            try {
                $response_datum = $this->invoke($local_message, $request);
                // if it's a one way message we only send the handshake if needed
                if ($this->local_protocol->messages[$remote_message_name]->is_one_way()) {
                    return ($buffer_writer->string() === "") ? null : $buffer_writer->string();
                }

            } catch (AvroRemoteException $e) {
                $error = $e;
            } catch (\Exception $e) {
                $error = new AvroRemoteException($e->getMessage());
            }

            $this->meta_writer->write($response_metadata, $encoder);
            $encoder->writeBoolean(!is_null($error));

            if (is_null($error)) {
                $datum_writer = new AvroIODatumWriter($local_message->response);
                $datum_writer->write($response_datum, $encoder);
            } else {
                $datum_writer = new AvroIODatumWriter($remote_message->errors);
                $datum_writer->write($error->getDatum(), $encoder);
            }

        } catch (AvroException $e) {
            $error = new AvroRemoteException($e->getMessage());
            $buffer_writer = new AvroStringIO();
            $encoder = new AvroIOBinaryEncoder($buffer_writer);
            $this->meta_writer->write($response_metadata, $encoder);
            $encoder->writeBoolean(!is_null($error));
            $datum_writer = new AvroIODatumWriter($this->system_error_schema);
            $datum_writer->write($error->getMessage(), $encoder);
        }

        return $buffer_writer->string();
    }

    /**
     * Processes an RPC handshake.
     * @param AvroIOBinaryDecoder $decoder Where to read from
     * @param AvroIOBinaryEncoder $encoder Where to write to.
     * @param Transceiver $transceiver the transceiver used for the response
     * @return AvroProtocol The requested Protocol.
     * @throws AvroProtocolParseException|AvroSchemaParseException
     */
    public function process_handshake(
        AvroIOBinaryDecoder $decoder,
        AvroIOBinaryEncoder $encoder,
        Transceiver $transceiver
    ) {
        if ($transceiver->is_connected()) {
            return $transceiver->get_remote();
        }

        $handshake_request = $this->handshake_responder_reader->read($decoder);
        $client_hash = $handshake_request["clientHash"];
        $client_protocol = $handshake_request["clientProtocol"];
        $remote_protocol = $this->get_protocol_cache($client_hash);

        if (is_null($remote_protocol) && !is_null($client_protocol)) {
            $remote_protocol = AvroProtocol::parse($client_protocol);
            $this->set_protocol_cache($client_hash, $remote_protocol);
        }

        $server_hash = $handshake_request["serverHash"];
        $handshake_response = array();

        if ($this->local_hash == $server_hash) {
            $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'BOTH';
        } else {
            $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'CLIENT';
        }

        $handshake_response["meta"] = null;
        if ($handshake_response['match'] !== 'BOTH') {
            $handshake_response["serverProtocol"] = $this->local_protocol->__toString();
            $handshake_response["serverHash"] = $this->local_hash;
        } else {
            $handshake_response["serverProtocol"] = null;
            $handshake_response["serverHash"] = null;
        }

        $this->handshake_responder_writer->write($handshake_response, $encoder);

        if ($handshake_response['match'] !== 'NONE') {
            $transceiver->set_remote($remote_protocol);
        }

        return $remote_protocol;
    }

    /**
     * @param string $hash hash of an Avro Protocol
     * @return AvroProtocol|null The protocol associated with $hash or null
     */
    public function get_protocol_cache($hash)
    {
        return $this->protocol_cache[$hash] ?? null;
    }

    /**
     * @param string $hash hash of an Avro Protocol
     * @param AvroProtocol $protocol
     * @return Responder $this
     */
    public function set_protocol_cache($hash, AvroProtocol $protocol)
    {
        $this->protocol_cache[$hash] = $protocol;
        return $this;
    }

    /**
     * Processes one procedure call
     * @param AvroProtocolMessage $local_message
     * @param mixed $request Call request
     * @return mixed Call response
     */
    abstract public function invoke($local_message, $request);
}
