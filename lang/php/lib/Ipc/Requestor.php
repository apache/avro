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
use Apache\Avro\Schema\AvroSchema;

/**
 * Base class for the client side of a protocol interaction.
 * @package Avro
 */
class Requestor
{
    /**
     * The transceiver used to send the request
     * @var Transceiver
     */
    protected $transceiver;

    protected $local_protocol;

    protected $remote_protocol = array();
    protected $remote_hash = array();
    protected $remote = null;
    /**
     * True if the Requestor need to send it's protocol to the remote server
     * @var boolean
     */
    protected $send_protocol = false;

    protected $handshake_requestor_writer;
    protected $handshake_requestor_reader;
    protected $meta_writer;
    protected $meta_reader;

    /**
     * Initializes a new requestor object
     * @param AvroProtocol $local_protocol Avro Protocol describing the messages sent and received.
     * @param Transceiver $transceiver Transceiver instance to channel messages through.
     */
    public function __construct(AvroProtocol $local_protocol, Transceiver $transceiver)
    {
        $this->local_protocol = $local_protocol;
        $this->transceiver = $transceiver;
        $this->handshake_requestor_writer = new AvroIODatumWriter(AvroSchema::parse(Responder::HANDSHAKE_REQUEST_SCHEMA_JSON));
        $this->handshake_requestor_reader = new AvroIODatumReader(AvroSchema::parse(Responder::HANDSHAKE_RESPONSE_SCHEMA_JSON));
        $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
        $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
    }

    public function local_protocol()
    {
        return $this->local_protocol;
    }

    public function transceiver()
    {
        return $this->trancseiver;
    }


    /**
     * Writes a request message and reads a response or error message.
     * @param string $message_name : name of the IPC method
     * @param mixed $request_datum : IPC request
     * @return bool
     * @return bool
     * @throws AvroException when $message_name is not registered on the local or remote protocol
     * @throws AvroRemoteException when server send an error
     */
    public function request($message_name, $request_datum)
    {
        $io = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($io);
        $this->write_handshake_request($encoder);
        $this->write_call_request($message_name, $request_datum, $encoder);

        $call_request = $io->string();
        if ($this->local_protocol->messages[$message_name]->is_one_way()) {
            $this->transceiver->writeMessage($call_request);
            if (!$this->transceiver->is_connected()) {
                $handshake_response = $this->transceiver->readMessage();
                $io = new AvroStringIO($handshake_response);
                $decoder = new AvroIOBinaryDecoder($io);
                $this->read_handshake_response($decoder);
            }
            return true;
        }

        $call_response = $this->transceiver->transceive($call_request);
        // process the handshake and call response
        $io = new AvroStringIO($call_response);
        $decoder = new AvroIOBinaryDecoder($io);
        $call_response_exists = $this->read_handshake_response($decoder);
        if ($call_response_exists) {
            $call_response = $this->read_call_response($message_name, $decoder);
            return $call_response;
        }

        return $this->request($message_name, $request_datum);
    }

    /**
     * Write the handshake request.
     * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
     */
    public function write_handshake_request(AvroIOBinaryEncoder $encoder)
    {
        if ($this->transceiver->is_connected()) {
            return;
        }

        $remote_name = $this->transceiver->remoteName();

        $local_hash = $this->local_protocol->md5();
        $remote_hash = $this->remote_hash[$remote_name] ?? null;
        if (is_null($remote_hash)) {
            $remote_hash = $local_hash;
            $this->remote = $this->local_protocol;
        } else {
            $this->remote = $this->remote_protocol[$remote_name];
        }

        $request_datum = ['clientHash' => $local_hash, 'serverHash' => $remote_hash, 'meta' => null];
        $request_datum["clientProtocol"] = ($this->send_protocol) ? $this->local_protocol->__toString() : null;

        $this->handshake_requestor_writer->write($request_datum, $encoder);
    }

    /**
     * The format of a call request is:
     * - request metadata, a map with values of type bytes
     * - the message name, an Avro string, followed by
     * - the message parameters. Parameters are serialized according to the message's request declaration.
     * @param string $message_name : name of the IPC method
     * @param mixed $request_datum : IPC request
     * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
     * @throws AvroException when $message_name is not registered on the local protocol
     */
    public function write_call_request($message_name, $request_datum, AvroIOBinaryEncoder $encoder)
    {
        $request_metadata = array();
        $this->meta_writer->write($request_metadata, $encoder);

        if (!isset($this->local_protocol->messages[$message_name])) {
            throw new AvroException("Unknown message: $message_name");
        }

        $encoder->writeString($message_name);
        $writer = new AvroIODatumWriter($this->local_protocol->messages[$message_name]->request);
        $writer->write($request_datum, $encoder);
    }

    /**
     * Reads and processes the handshake response message.
     * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
     * @return boolean true if a response exists.
     * @throws  AvroException when server respond an unknown handshake match
     */
    public function read_handshake_response(AvroIOBinaryDecoder $decoder)
    {
        // if the handshake has been successfully made previously,
        // no need to do it again
        if ($this->transceiver->is_connected()) {
            return true;
        }

        $handshake_response = $this->handshake_requestor_reader->read($decoder);
        $match = $handshake_response["match"];

        switch ($match) {
            case 'BOTH':
                $established = true;
                $this->send_protocol = false;
                break;
            case 'CLIENT':
                $established = true;
                $this->send_protocol = false;
                $this->set_remote($handshake_response);
                break;
            case 'NONE':
                $this->send_protocol = true;
                $this->set_remote($handshake_response);
                $established = false;
                break;
            default:
                throw new AvroException("Bad handshake response match: $match");
        }

        if ($established) {
            $this->transceiver->set_remote($this->remote);
        }

        return $established;
    }

    /**
     * @param $handshake_response
     * @throws \Apache\Avro\Protocol\AvroProtocolParseException|\Apache\Avro\Schema\AvroSchemaParseException
     */
    protected function set_remote($handshake_response)
    {
        $this->remote_protocol[$this->transceiver->remoteName()] = AvroProtocol::parse(
            $handshake_response["serverProtocol"]
        );
        if (!isset($this->remote_hash[$this->transceiver->remoteName()])) {
            $this->remote_hash[$this->transceiver->remoteName()] = $handshake_response["serverHash"];
        }
    }

    /**
     * Reads and processes a method call response.
     * The format of a call response is:
     * - response metadata, a map with values of type bytes
     * - a one-byte error flag boolean, followed by either:
     * - if the error flag is false, the message response, serialized per the message's response schema.
     * - if the error flag is true, the error, serialized per the message's error union schema.
     * @param string $message_name : name of the IPC method
     * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
     * @return boolean true if a response exists.
     * @throws  AvroException $message_name is not registered on the local or remote protocol
     * @throws AvroRemoteException when server send an error
     */
    public function read_call_response($message_name, AvroIOBinaryDecoder $decoder)
    {
        $response_metadata = $this->meta_reader->read($decoder);

        if (!isset($this->remote->messages[$message_name])) {
            throw new AvroException("Unknown remote message: $message_name");
        }
        $remote_message_schema = $this->remote->messages[$message_name];

        if (!isset($this->local_protocol->messages[$message_name])) {
            throw new AvroException("Unknown local message: $message_name");
        }
        $local_message_schema = $this->local_protocol->messages[$message_name];

        // No error raised on the server
        if (!$decoder->readBoolean()) {
            $datum_reader = new AvroIODatumReader($remote_message_schema->response, $local_message_schema->response);
            return $datum_reader->read($decoder);
        }

        $datum_reader = new AvroIODatumReader($remote_message_schema->errors, $local_message_schema->errors);
        throw new AvroRemoteException($datum_reader->read($decoder));
    }
}
