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
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a socket server
 */
class SocketTransceiver extends Transceiver
{
    protected $socket;
    
    protected function __construct()
    {
    }

    /**
     * Create a SocketTransceiver with a new socket connected to $host:$port
     * @param string $host
     * @param int $host
     * @return SocketTransceiver
     */
    public static function create($host, $port)
    {
        $transceiver = new SocketTransceiver();
        $transceiver->createSocket($host, $port);

        return $transceiver;
    }

    /**
     * @param $host
     * @param $port
     * @throws AvroRemoteException
     */
    protected function createSocket($host, $port)
    {
        $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
        if (@socket_connect($this->socket, $host, $port) === false) {
            throw new AvroRemoteException("Connection failed on $host:$port.");
        }
    }

    /**
     * Create a SocketTransceiver based on the existing $socket
     * @param resource $socket
     * @return SocketTransceiver
     */
    public static function accept($socket)
    {
        $transceiver = new SocketTransceiver();
        $transceiver->acceptSocket($socket);
        return $transceiver;
    }

    protected function acceptSocket($socket)
    {
        $this->socket = socket_accept($socket);
    }

    /**
     * Reads a single message from the channel.
     * Blocks until a message can be read.
     * @return string The message read from the channel.
     */
    public function readMessage(): ?string
    {
        $message = "";
        while (socket_recv($this->socket, $buf, 4, MSG_WAITALL)) {
            if ($buf === null) {
                return $buf;
            }
            $frame_size = unpack("Nsize", $buf);
            $frame_size = $frame_size["size"];
            if ($frame_size === 0) {
                return $message;
            }

            socket_recv($this->socket, $buf, $frame_size, MSG_WAITALL);
            $message .= $buf;
        }

        return $message;
    }

    /**
     * Writes a message into the channel. Blocks until the message has been written.
     * @param string $message
     */
    public function writeMessage($message): void
    {
        $binary_length = strlen($message);

        $max_binary_frame_length = 8192 - 4;
        $sended_length = 0;

        $frames = [];
        while ($sended_length < $binary_length) {
            $not_sended_length = $binary_length - $sended_length;
            $binary_frame_length = ($not_sended_length > $max_binary_frame_length)
                ? $max_binary_frame_length
                : $not_sended_length;
            $frames[] = substr($message, $sended_length, $binary_frame_length);
            $sended_length += $binary_frame_length;
        }

        foreach ($frames as $frame) {
            $msg = pack("N", strlen($frame)) . $frame;
            socket_write($this->socket, $msg, strlen($msg));
        }
        $footer = pack("N", 0);
        //socket_send ($this->socket, $header, strlen($header) , 0);
        socket_write($this->socket, $footer, strlen($footer));
    }


    /**
     * Check if this transceiver has proceed to a valid handshake exchange
     * @return boolean true if this transceiver has make a valid hanshake with it's remote
     */
    public function is_connected()
    {
        return (!is_null($this->remote));
    }

    /**
     * Return the name of the socket remode side
     * @return string the remote name
     */
    public function remoteName(): ?string
    {
        $result = null;
        if ($this->socket !== null) {
            $result = socket_getpeername($this->socket, $address, $port);
        }

        return ($result) ? "$address:$port" : null;
    }

    public function close()
    {
        socket_close($this->socket);
    }

    public function socket()
    {
        return $this->socket;
    }
}
