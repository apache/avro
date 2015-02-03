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
package org.apache.avro.specific;

import org.apache.avro.test.http.*;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSpecificBuilderTree {

  private Request.Builder createPartialBuilder() {
    Request.Builder requestBuilder = Request.newBuilder();
    requestBuilder.setTimestamp(1234567890);

    requestBuilder
      .getConnectionBuilder()
        .setNetworkType(NetworkType.IPv4);

    requestBuilder
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setUseragent("Chrome 123")
          .setId("Foo");

    requestBuilder
      .getHttpRequestBuilder()
        .getURIBuilder()
          .setMethod(HttpMethod.GET)
          .setPath("/index.html");

    if (!requestBuilder
           .getHttpRequestBuilder()
             .getURIBuilder()
               .hasParameters()) {
      requestBuilder
        .getHttpRequestBuilder()
          .getURIBuilder()
            .setParameters(new ArrayList<QueryParameter>());
    }

    requestBuilder
      .getHttpRequestBuilder()
        .getURIBuilder()
          .getParameters()
            .add(QueryParameter.newBuilder().setName("Foo").setValue("Bar").build());

    return requestBuilder;
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void failOnIncompleteTree() {
    Request.Builder requestBuilder = createPartialBuilder();
    Request request = requestBuilder.build();
    fail("Should NEVER get here");
  }

  @Test
  public void copyBuilder() {
    Request.Builder requestBuilder1 = createPartialBuilder();

    Request.Builder requestBuilder2 = Request.newBuilder(requestBuilder1);

    requestBuilder1
      .getConnectionBuilder()
        .setNetworkAddress("1.1.1.1");

    requestBuilder2
      .getConnectionBuilder()
        .setNetworkAddress("2.2.2.2");

    requestBuilder2
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setId("Bar");

    Request request1 = requestBuilder1.build();
    Request request2 = requestBuilder2.build();

    assertEquals(NetworkType.IPv4,  request1.getConnection().getNetworkType());
    assertEquals("1.1.1.1",         request1.getConnection().getNetworkAddress());
    assertEquals("Chrome 123",      request1.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Foo",             request1.getHttpRequest().getUserAgent().getId());
    assertEquals(HttpMethod.GET,    request1.getHttpRequest().getURI().getMethod());
    assertEquals("/index.html",     request1.getHttpRequest().getURI().getPath());
    assertEquals(1,                 request1.getHttpRequest().getURI().getParameters().size());
    assertEquals("Foo",             request1.getHttpRequest().getURI().getParameters().get(0).getName());
    assertEquals("Bar",             request1.getHttpRequest().getURI().getParameters().get(0).getValue());

    assertEquals(NetworkType.IPv4,  request2.getConnection().getNetworkType());
    assertEquals("2.2.2.2",         request2.getConnection().getNetworkAddress());
    assertEquals("Chrome 123",      request2.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Bar",             request2.getHttpRequest().getUserAgent().getId());
    assertEquals(HttpMethod.GET,    request2.getHttpRequest().getURI().getMethod());
    assertEquals("/index.html",     request2.getHttpRequest().getURI().getPath());
    assertEquals(1,                 request2.getHttpRequest().getURI().getParameters().size());
    assertEquals("Foo",             request2.getHttpRequest().getURI().getParameters().get(0).getName());
    assertEquals("Bar",             request2.getHttpRequest().getURI().getParameters().get(0).getValue());
  }

  @Test
  public void createBuilderFromInstance(){
    Request.Builder requestBuilder1 = createPartialBuilder();
    requestBuilder1
      .getConnectionBuilder()
        .setNetworkAddress("1.1.1.1");

    Request request1 = requestBuilder1.build();

    Request.Builder requestBuilder2 = Request.newBuilder(request1);

    requestBuilder2
      .getConnectionBuilder()
        .setNetworkAddress("2.2.2.2");

    requestBuilder2
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setId("Bar");

    requestBuilder2
      .getHttpRequestBuilder()
        .getURIBuilder()
          .setMethod(HttpMethod.POST);

    requestBuilder2
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setUseragent("Firefox 456");

    Request request2 = requestBuilder2.build();

    assertEquals(NetworkType.IPv4,  request1.getConnection().getNetworkType());
    assertEquals("1.1.1.1",         request1.getConnection().getNetworkAddress());
    assertEquals("Chrome 123",      request1.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Foo",             request1.getHttpRequest().getUserAgent().getId());
    assertEquals(HttpMethod.GET,    request1.getHttpRequest().getURI().getMethod());
    assertEquals("/index.html",     request1.getHttpRequest().getURI().getPath());
    assertEquals(1,                 request1.getHttpRequest().getURI().getParameters().size());
    assertEquals("Foo",             request1.getHttpRequest().getURI().getParameters().get(0).getName());
    assertEquals("Bar",             request1.getHttpRequest().getURI().getParameters().get(0).getValue());

    assertEquals(NetworkType.IPv4,  request2.getConnection().getNetworkType());
    assertEquals("2.2.2.2",         request2.getConnection().getNetworkAddress());
    assertEquals("Firefox 456",     request2.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Bar",             request2.getHttpRequest().getUserAgent().getId());
    assertEquals(HttpMethod.POST,   request2.getHttpRequest().getURI().getMethod());
    assertEquals("/index.html",     request2.getHttpRequest().getURI().getPath());
    assertEquals(1,                 request2.getHttpRequest().getURI().getParameters().size());
    assertEquals("Foo",             request2.getHttpRequest().getURI().getParameters().get(0).getName());
    assertEquals("Bar",             request2.getHttpRequest().getURI().getParameters().get(0).getValue());
  }

  private Request.Builder createLastOneTestsBuilder() {
    Request.Builder requestBuilder = Request.newBuilder();
    requestBuilder.setTimestamp(1234567890);

    requestBuilder
      .getConnectionBuilder()
        .setNetworkType(NetworkType.IPv4)
        .setNetworkAddress("1.1.1.1");

    return requestBuilder;
  }

  @Test
  public void lastOneWins_Setter() {
    Request.Builder requestBuilder = createLastOneTestsBuilder();

    requestBuilder
      .getHttpRequestBuilder()
        .getURIBuilder()
          .setMethod(HttpMethod.GET)
          .setPath("/index.html");

    requestBuilder
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setUseragent("Chrome 123")
          .setId("Foo");

    HttpRequest httpRequest = HttpRequest.newBuilder()
            .setUserAgent(new UserAgent("Bar","Firefox 321"))
            .setURI(HttpURI.newBuilder()
                    .setMethod(HttpMethod.POST)
                    .setPath("/login.php")
                    .build())
            .build();

    Request request = requestBuilder.setHttpRequest(httpRequest).build();

    assertEquals(NetworkType.IPv4,  request.getConnection().getNetworkType());
    assertEquals("1.1.1.1",         request.getConnection().getNetworkAddress());
    assertEquals(0,                 request.getHttpRequest().getURI().getParameters().size());

    assertEquals("Firefox 321",     request.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Bar",             request.getHttpRequest().getUserAgent().getId());
    assertEquals(HttpMethod.POST,   request.getHttpRequest().getURI().getMethod());
    assertEquals("/login.php",      request.getHttpRequest().getURI().getPath());
  }

  @Test
  public void lastOneWins_Builder() {
    Request.Builder requestBuilder = createLastOneTestsBuilder();

    HttpRequest httpRequest = HttpRequest.newBuilder()
            .setUserAgent(new UserAgent("Bar", "Firefox 321"))
            .setURI(HttpURI.newBuilder()
                    .setMethod(HttpMethod.POST)
                    .setPath("/login.php")
                    .build())
            .build();
    requestBuilder.setHttpRequest(httpRequest);

    requestBuilder
      .getHttpRequestBuilder()
        .getURIBuilder()
          .setMethod(HttpMethod.GET)
          .setPath("/index.html");

    requestBuilder
      .getHttpRequestBuilder()
        .getUserAgentBuilder()
          .setUseragent("Chrome 123")
          .setId("Foo");

    Request request = requestBuilder.build();

    assertEquals(NetworkType.IPv4,  request.getConnection().getNetworkType());
    assertEquals("1.1.1.1",         request.getConnection().getNetworkAddress());
    assertEquals("Chrome 123",      request.getHttpRequest().getUserAgent().getUseragent());
    assertEquals("Foo",             request.getHttpRequest().getUserAgent().getId());
    assertEquals(0,                 request.getHttpRequest().getURI().getParameters().size());

    assertEquals(HttpMethod.GET,    request.getHttpRequest().getURI().getMethod());
    assertEquals("/index.html",     request.getHttpRequest().getURI().getPath());
  }

}
