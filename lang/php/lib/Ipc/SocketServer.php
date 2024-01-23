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

/**
 * Socket server implementation.
 */
class SocketServer
{
    /** @var Responder */
    protected $responder;
    protected $socket;
    protected $use_netty_framed_transceiver;

    /**
     * SocketServer constructor.
     * @param $host
     * @param $port
     * @param $responder
     * @param false $use_netty_framed_transceiver
     */
    public function __construct($host, $port, $responder, $use_netty_framed_transceiver = false)
    {
        $this->responder = $responder;
        $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
        $this->use_netty_framed_transceiver = $use_netty_framed_transceiver;
        socket_bind($this->socket, $host, $port);
        socket_listen($this->socket, 3);
    }

    /**
     * @param int $max_clients
     */
    public function start($max_clients = 10): void
    {
        $transceivers = [];

        while (true) {

            // $read contains all the client we listen to
            $read = [$this->socket];
            for ($i = 0; $i < $max_clients; $i++) {
                if (isset($transceivers[$i]) && $transceivers[$i] !== null) {
                    $read[$i + 1] = $transceivers[$i]->socket();
                }
            }

            // check all client to know which ones are writing
            $ready = socket_select($read, $write, $except, null);
            // $read contains all client that send something to the server

            // New connexion
            if (in_array($this->socket, $read)) {
                for ($i = 0; $i < $max_clients; $i++) {
                    if (!isset($transceivers[$i])) {
                        $transceivers[$i] = ($this->use_netty_framed_transceiver)
                            ? NettyFramedSocketTransceiver::accept($this->socket)
                            : SocketTransceiver::accept($this->socket);
                        break;
                    }
                }
            }

            // Check all client that are trying to write
            for ($i = 0; $i < $max_clients; $i++) {
                if (isset($transceivers[$i]) && in_array($transceivers[$i]->socket(), $read)) {
                    $is_closed = $this->handleRequest($transceivers[$i]);
                    if ($is_closed) {
                        unset($transceivers[$i]);
                    }
                }
            }
        }

        socket_close($this->socket);
    }

    /**
     * @param Transceiver $transceiver
     * @return bool|null
     * @throws AvroException
     */
    public function handleRequest(Transceiver $transceiver): ?bool
    {
        // Read the message
        $call_request = $transceiver->readMessage();

        // Respond if the message is not empty
        if (!is_null($call_request)) {
            $call_response = $this->responder->respond($call_request, $transceiver);
            if (!is_null($call_response)) {
                $transceiver->writeMessage($call_response);
            }
            return false;

            // Else the client has disconnect
        }

        $transceiver->close();
        return true;
    }
}
