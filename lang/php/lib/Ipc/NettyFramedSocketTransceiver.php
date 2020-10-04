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
 * This class can be used by a client to communicate with a socket server (netty implementation)
 */
class NettyFramedSocketTransceiver extends SocketTransceiver
{
    protected static $serial = 1;

    /**
     * Create a SocketTransceiver with a new socket connected to $host:$port
     * @param string $host
     * @param int $host
     * @return NettyFramedSocketTransceiver
     */
    public static function create($host, $port)
    {
        $transceiver = new self();
        $transceiver->createSocket($host, $port);

        return $transceiver;
    }

    /**
     * Create a SocketTransceiver based on the existing $socket
     * @param resource $socket
     * @return NettyFramedSocketTransceiver
     */
    public static function accept($socket)
    {
        $transceiver = new self();
        $transceiver->acceptSocket($socket);

        return $transceiver;
    }

    /**
     * Reads a single message from the channel.
     * Blocks until a message can be read.
     * @return string The message read from the channel.
     */
    public function readMessage(): ?string
    {
        socket_recv($this->socket, $buf, 8, MSG_WAITALL);
        if ($buf === null) {
            return $buf;
        }

        $frame_count = unpack("Nserial/Ncount", $buf);
        $frame_count = $frame_count["count"];
        $message = "";
        for ($i = 0; $i < $frame_count; $i++) {
            socket_recv($this->socket, $buf, 4, MSG_WAITALL);
            $frame_size = unpack("Nsize", $buf);
            $frame_size = $frame_size["size"];
            if ($frame_size > 0) {
                socket_recv($this->socket, $bif, $frame_size, MSG_WAITALL);
                $message .= $bif;
            }

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

        $header = pack("N", self::$serial++) . pack("N", count($frames));
        //socket_send ($this->socket, $header, strlen($header) , 0);
        $x = socket_write($this->socket, $header, strlen($header));
        foreach ($frames as $frame) {
            $msg = pack("N", strlen($frame)) . $frame;
            if (@socket_write($this->socket, $msg, strlen($msg)) === false) {
                throw new AvroRemoteException(socket_strerror(socket_last_error()));
            }
        }
    }
}
