/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.message;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.ipc.reflect.ReflectResponder;

import org.apache.avro.message.protocol.BDRequest;
import org.apache.avro.message.protocol.BDResponse;
import org.apache.avro.message.protocol.BigDecimalConversionUpgrade;
import org.apache.avro.message.protocol.BigDecimalConversionOld;
import org.apache.avro.message.protocol.BigDecimalProtocol;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBigDecimalEvolution {

  @Test
  public void bigDecimalEvolutionThroughLogicalType() throws Exception {
    ReflectData oldApp = new ReflectData();
    oldApp.addLogicalTypeConversion(new BigDecimalConversionOld());
    ReflectData upgradeApp = new ReflectData();
    upgradeApp.addLogicalTypeConversion(new BigDecimalConversionUpgrade());

    verify(oldApp, oldApp);
    verify(upgradeApp, upgradeApp);
    verify(upgradeApp, oldApp);
    verify(oldApp, upgradeApp);
  }

  private void verify(ReflectData clientData, ReflectData serverData) throws IOException {
    Protocol clientProtocol = serverData.getProtocol(BigDecimalProtocol.class);
    ReflectResponder clientResponder = new ReflectResponder(clientProtocol, null, serverData);
    ReflectRequestor clientRequestor = new ReflectRequestor(clientProtocol, new LocalTransceiver(clientResponder),
        serverData);
    Schema clientRequestSchema = serverData.getSchema(BDRequest.class);
    Schema clientResponseSchema = serverData.getSchema(BDResponse.class);

    Protocol serverProtocol = clientData.getProtocol(BigDecimalProtocol.class);
    ReflectResponder serverResponder = new ReflectResponder(serverProtocol, null, clientData);
    ReflectRequestor serverRequestor = new ReflectRequestor(serverProtocol, new LocalTransceiver(serverResponder),
        clientData);
    Schema serverRequestSchema = clientData.getSchema(BDRequest.class);
    Schema serverResponseSchema = clientData.getSchema(BDResponse.class);

    BDRequest originalRequest = new BDRequest();
    originalRequest.setValue(new BigDecimal("123.54321"));

    BDRequest request = verifyRequest(originalRequest, serverRequestSchema, clientRequestSchema, serverProtocol,
        serverRequestor, clientResponder);

    BDResponse originalResponse = new BDResponse(request.getValue());
    verifyResponse(originalResponse, serverResponseSchema, clientResponseSchema, serverResponder, clientRequestor);
  }

  private BDRequest verifyRequest(BDRequest originalRequest, Schema writerSchema, Schema readerSchema,
      Protocol writerProtocol, ReflectRequestor requestor, ReflectResponder responder) throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    requestor.writeRequest(writerProtocol.getMessages().get("test").getRequest(), new Object[] { originalRequest },
        encoder);

    encoder.flush();

    Object obj = responder.readRequest(writerSchema, readerSchema,
        DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertTrue(obj instanceof BDRequest);
    BDRequest request = (BDRequest) obj;

    assertEquals(originalRequest.getValue(), request.getValue());
    return request;
  }

  private void verifyResponse(BDResponse originalResponse, Schema writerSchema, Schema readerSchema,
      ReflectResponder responder, ReflectRequestor requestor) throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    responder.writeResponse(writerSchema, originalResponse, encoder);

    encoder.flush();

    Object obj = requestor.readResponse(writerSchema, readerSchema,
        DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertTrue(obj instanceof BDResponse);

    BDResponse response = (BDResponse) obj;
    assertEquals(originalResponse.getValue(), response.getValue());
  }

}
