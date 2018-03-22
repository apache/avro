/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.compress.utils.IOUtils;

/** Private helper for creating codecs that have OutputStream implementations
 * for compressing data and InputStream implementations for decompression. **/
abstract class OutputInputStreamCodec extends OutputStreamCodec {
  private static final int BUFFER_SIZE = 32 * 1024;

  /** Return an InputStream that uncompresses the provided input.
   * @throws IOException **/
  protected abstract InputStream uncompressedStream(InputStream input)
      throws IOException;

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {

    int compressedSize = data.remaining();
    ByteArrayOutputStream baos = getOutputBuffer(compressedSize);
    InputStream bytesIn = new ByteArrayInputStream(
      data.array(),
      data.arrayOffset() + data.position(),
      compressedSize);
    InputStream ios = uncompressedStream(bytesIn);
    try {
      IOUtils.copy(ios, baos, BUFFER_SIZE);
    } finally {
      ios.close();
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }
}
