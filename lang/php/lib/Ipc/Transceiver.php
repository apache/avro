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

/**
 * Abstract class to handle communicaton (framed read & write) between client & server
 */
abstract class Transceiver
{
    protected $remote;

    /**
     * Processes a single request-reply interaction. Synchronous request-reply interaction.
     * @param string $request the request message
     * @return string the reply message
     */
    public function transceive($request)
    {
        $this->writeMessage($request);
        return $this->readMessage();
    }

    /**
     * Writes a message into the channel. Blocks until the message has been written.
     * @param string $message
     */
    abstract public function writeMessage($message): void;

    /**
     * Reads a single message from the channel.
     * Blocks until a message can be read.
     * @return string The message read from the channel.
     */
    abstract public function readMessage(): ?string;

    public function get_remote()
    {
        return $this->remote;
    }

    public function set_remote($remote)
    {
        $this->remote = $remote;
        return $this;
    }

    /**
     * Close this transceiver
     */
    abstract public function close();

    /**
     * Check if this transceiver has proceed to a valid handshake exchange
     * @return boolean true if this transceiver has make a valid hanshake with it's remote
     */
    abstract public function is_connected();
}
