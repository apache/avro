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
package org.apache.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.test.Simple;

import org.junit.Test;

import org.mortbay.jetty.security.SslSocketConnector;

import java.net.URL;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;

public class TestProtocolHttps extends TestProtocolSpecific {

  @Override
  public Server createServer(Responder testResponder) throws Exception {
    System.setProperty("javax.net.ssl.keyStore", "src/test/keystore");
    System.setProperty("javax.net.ssl.keyStorePassword", "avrotest");
    System.setProperty("javax.net.ssl.password", "avrotest");
    System.setProperty("javax.net.ssl.trustStore", "src/test/truststore");
    System.setProperty("javax.net.ssl.trustStorePassword", "avrotest");
    SslSocketConnector connector = new SslSocketConnector();
    connector.setPort(18443);
    connector.setKeystore(System.getProperty("javax.net.ssl.keyStore"));
    connector.setPassword(System.getProperty("javax.net.ssl.password"));
    connector.setKeyPassword(System.getProperty("javax.net.ssl.keyStorePassword"));
    connector.setHost("localhost");
    connector.setNeedClientAuth(false);
    return new HttpServer(testResponder, connector);
  }
  
  @Override
  public Transceiver createTransceiver() throws Exception{
    return new HttpTransceiver(new URL("https://localhost:"+server.getPort()+"/"));
  }
 
  protected int getExpectedHandshakeCount() {
    return REPEATING;
  }

}
