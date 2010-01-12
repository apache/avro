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

package org.apache.avro.ipc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.net.URL;
import java.net.URLConnection;

/** An HTTP-based {@link Transceiver} implementation. */
public class HttpTransceiver extends Transceiver {
  static final String CONTENT_TYPE = "avro/binary"; 

  private URL url;
  private URLConnection connection;
  
  public HttpTransceiver(URL url) { this.url = url; }

  public String getRemoteName() { return this.url.toString(); }

  @Override
  public synchronized List<ByteBuffer> transceive(List<ByteBuffer> request)
    throws IOException {
    this.connection = url.openConnection();
    connection.setRequestProperty("Content-Type", CONTENT_TYPE);
    connection.setRequestProperty("Content-Length",
                                  Integer.toString(getLength(request)));
    connection.setDoOutput(true);
    //LOG.info("Connecting to: "+url);
    return super.transceive(request);
  }

  public synchronized List<ByteBuffer> readBuffers() throws IOException {
    return readBuffers(connection.getInputStream());
  }

  public synchronized void writeBuffers(List<ByteBuffer> buffers)
    throws IOException {
    writeBuffers(buffers, connection.getOutputStream());
  }

  static int getLength(List<ByteBuffer> buffers) {
    int length = 0;
    for (ByteBuffer buffer : buffers) {
      length += 4;
      length += buffer.remaining();
    }
    length += 4;
    return length;
  }

  static List<ByteBuffer> readBuffers(InputStream in)
    throws IOException {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    while (true) {
      int length = (in.read()<<24)+(in.read()<<16)+(in.read()<<8)+in.read();
      if (length == 0) {                       // end of buffers
        return buffers;
      }
      ByteBuffer buffer = ByteBuffer.allocate(length);
      while (buffer.hasRemaining()) {
        int p = buffer.position();
        int i = in.read(buffer.array(), p, buffer.remaining());
        if (i < 0)
          throw new EOFException("Unexpected EOF");
        buffer.position(p+i);
      }
      buffer.flip();
      buffers.add(buffer);
    }
  }

  static void writeBuffers(List<ByteBuffer> buffers, OutputStream out)
    throws IOException {
    for (ByteBuffer buffer : buffers) {
      writeLength(buffer.limit(), out);           // length-prefix
      out.write(buffer.array(), buffer.position(), buffer.remaining());
      buffer.position(buffer.limit());
    }
    writeLength(0, out);                          // null-terminate
  }

  private static void writeLength(int length, OutputStream out)
    throws IOException {
    out.write(0xff & (length >>> 24));
    out.write(0xff & (length >>> 16));
    out.write(0xff & (length >>> 8));
    out.write(0xff & length);
  }
}

