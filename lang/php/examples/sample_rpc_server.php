#!/usr/bin/env php
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

use Apache\Avro\Ipc\AvroRemoteException;
use Apache\Avro\Ipc\Responder;
use Apache\Avro\Ipc\SocketServer;
use Apache\Avro\Protocol\AvroProtocol;

require_once __DIR__ . '/../vendor/autoload.php';

$protocol = <<<PROTO
{
 "namespace": "examples.protocol",
 "protocol": "TestProtocol",

 "types": [
     {"type": "record", "name": "SimpleRequest",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "SimpleResponse",
      "fields": [{"name": "response",   "type": "string"}]
     },
     {"type": "record", "name": "Notification",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "RaiseException",
      "fields": [{"name": "cause",   "type": "string"}]
     },
     {"type": "record", "name": "NeverSend",
      "fields": [{"name": "never",   "type": "string"}]
     },
     {"type": "error", "name": "AlwaysRaised",
      "fields": [{"name": "exception",   "type": "string"}]
     }
 ],

 "messages": {
     "testSimpleRequestResponse": {
         "doc" : "Simple Request Response",
         "request": [{"name": "message", "type": "SimpleRequest"}],
         "response": "SimpleResponse"
     },
     "testSimpleRequestWithoutParameters": {
         "doc" : "Simple Request Response",
         "request": [],
         "response": "SimpleResponse"
     },
     "testNotification": {
         "doc" : "Notification : one-way message",
         "request": [{"name": "notification", "type": "Notification"}],
         "one-way": true
     },
     "testRequestResponseException": {
         "doc" : "Request Response with Exception",
         "request": [{"name": "exception", "type": "RaiseException"}],
         "response" : "NeverSend",
         "errors" : ["AlwaysRaised"]
     }
 }
}
PROTO;

class TestProtocolResponder extends Responder
{
    public function invoke($local_message, $request)
    {
        echo $local_message->name . ":" . json_encode($request) . "\n";
        switch ($local_message->name) {

            case "testSimpleRequestResponse":
                if ($request["message"]["subject"] == "ping") {
                    return array("response" => "pong");
                }

                if ($request["message"]["subject"] == "pong") {
                    return array("response" => "ping");
                }
                break;

            case "testSimpleRequestWithoutParameters":
                return array("response" => "no incoming parameters");
                break;

            case "testNotification":
                break;

            case "testRequestResponseException":
                if ($request["exception"]["cause"] === "callback") {
                    throw new AvroRemoteException(array("exception" => "raised on callback cause"));
                }

                throw new AvroRemoteException("System exception");
            default:
                throw new AvroRemoteException("Method unknown");
        }
    }
}

$server = new SocketServer('127.0.0.1', 1411, new TestProtocolResponder(AvroProtocol::parse($protocol)), true);
$server->start();
