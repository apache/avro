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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro\Tests;

use Apache\Avro\Ipc\AvroRemoteException;
use Apache\Avro\Ipc\Requestor;
use Apache\Avro\Ipc\Responder;
use Apache\Avro\Ipc\SocketTransceiver;
use Apache\Avro\Protocol\AvroProtocol;
use PHPUnit\Framework\TestCase;

/**
 * Basic Transceiver to connect to a TestServer
 * (no transport involved)
 */
class TestTransceiver extends SocketTransceiver
{

    public $server;
    protected $response;

    public function __construct()
    {
    }

    public static function getTestClient($server)
    {
        $transceiver = new TestTransceiver();
        $transceiver->server = $server;

        return $transceiver;
    }

    public function readMessage(): ?string
    {
        return $this->response;
    }

    /**
     * Writes a message into the channel. Blocks until the message has been written.
     * @param string $message
     */
    public function writeMessage($message): void
    {
        $this->response = null;
        if (!is_null($this->server)) {
            $this->response = $this->server->start($message);
        }
    }

    /**
     * Return the name of the socket remode side
     * @return string the remote name
     */
    public function remoteName(): ?string
    {
        return 'TestTransceiver';
    }
}

/**
 * Basic test server that only call it's responder invoke function
 */
class TestServer
{

    public $responder;
    public $transceiver;

    public function __construct(Responder $responder)
    {
        $this->responder = $responder;
        $this->transceiver = new TestTransceiver();
    }

    public function start($call_request)
    {
        $call_response = $this->responder->respond($call_request, $this->transceiver);
        if (!is_null($call_response)) {
            return $call_response;
        }
    }
}

class TestProtocolResponder extends Responder
{
    public function invoke($local_message, $request)
    {
        switch ($local_message->name) {
            case "testSimpleRequestResponse":
                if ($request["message"]["subject"] === "ping") {
                    return array("response" => "pong");
                }

                if ($request["message"]["subject"] === "pong") {
                    return array("response" => "ping");
                }
                break;

            case "testSimpleRequestParams":
                return array("response" => "ping");
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
                break;

            default:
                throw new AvroRemoteException("Method unknown");
        }
    }
}

class IpcTest extends TestCase
{
    private $protocol = <<<PROTO
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

    public function testSimpleRequestResponse()
    {
        $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
        $client = TestTransceiver::getTestClient($server);
        $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);

        $response = $requestor->request('testSimpleRequestResponse', ["message" => ["subject" => "ping"]]);
        $this->assertEquals("pong", $response["response"]);
        $response = $requestor->request('testSimpleRequestResponse', ["message" => ["subject" => "pong"]]);
        $this->assertEquals("ping", $response["response"]);
    }

    public function testNotification()
    {
        $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
        $client = TestTransceiver::getTestClient($server);
        $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
        $response = $requestor->request('testNotification', ["notification" => ["subject" => "notify"]]);
        $this->assertTrue($response);
    }

    public function testHandshake()
    {
        $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
        $client = TestTransceiver::getTestClient($server);
        $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
        $this->assertFalse($client->is_connected());
        $this->assertFalse($server->transceiver->is_connected());

        $response = $requestor->request('testNotification', ["notification" => ["subject" => "notify"]]);
        $this->assertTrue($response);
        $this->assertTrue($client->is_connected());
        $this->assertTrue($server->transceiver->is_connected());

        $response = $requestor->request('testSimpleRequestResponse', ["message" => ["subject" => "ping"]]);
        $this->assertEquals("pong", $response["response"]);
        $this->assertTrue($client->is_connected());
        $this->assertTrue($server->transceiver->is_connected());
    }

    public function testRequestResponseException()
    {
        $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
        $client = TestTransceiver::getTestClient($server);
        $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);

        $exception_raised = false;
        try {
            $requestor->request('testRequestResponseException', ["exception" => ["cause" => "callback"]]);
        } catch (AvroRemoteException $e) {
            $exception_raised = true;
            $exception_datum = $e->getDatum();
            $this->assertEquals("raised on callback cause", $exception_datum["exception"]);
        }

        $this->assertTrue($exception_raised);
        $exception_raised = false;
        try {
            $requestor->request('testRequestResponseException', ["exception" => ["cause" => "system"]]);
        } catch (AvroRemoteException $e) {
            $exception_raised = true;
            $exception_datum = $e->getDatum();
            $this->assertEquals("System exception", $exception_datum);
        }
        $this->assertTrue($exception_raised);
    }
}
