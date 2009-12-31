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
package org.apache.avro.tool;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRequestor;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.ipc.HttpTransceiver;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Sends a single RPC message.
 */
public class RpcSendTool implements Tool {
  @Override
  public String getName() {
    return "rpcsend";
  }

  @Override
  public String getShortDescription() {
    return "Sends a single RPC message.";
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 5) {
      err.println(
          "Expected 5 arguments: protocol message_name host port json_data");
      return 1;
    }
    Protocol protocol = Protocol.parse(args.get(0));
    String messageName = args.get(1);
    Message message = protocol.getMessages().get(messageName);
    if (message == null) {
      err.println(String.format("No message named '%s' found in protocol '%s'.",
          messageName, protocol));
      return 1;
    }
    String host = args.get(2);
    int port = Integer.parseInt(args.get(3));
    String jsonData = args.get(4);
    
    Object datum = Util.jsonToGenericDatum(message.getRequest(), jsonData);
    GenericRequestor client = makeClient(protocol, host, port);
    Object response = client.request(message.getName(), datum);
    dumpJson(out, message.getResponse(), response);
    return 0;
  }

  private void dumpJson(PrintStream out, Schema schema, Object datum) 
  throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
    JsonGenerator g =
      new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
    g.useDefaultPrettyPrinter();
    writer.write(datum, new JsonEncoder(schema, g));
    g.flush();
    out.println();
    out.flush();
  }

  private GenericRequestor makeClient(Protocol protocol, String host, int port) 
  throws IOException {
    HttpTransceiver transceiver = 
      new HttpTransceiver(new URL("http", host, port, "/"));
    GenericRequestor requestor = new GenericRequestor(protocol, transceiver);
    return requestor;
  }
}
