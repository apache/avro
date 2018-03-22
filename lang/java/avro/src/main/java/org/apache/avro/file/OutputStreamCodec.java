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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Private helper for creating codecs that have OutputStream
 * implementations for compressing data **/
abstract class OutputStreamCodec extends Codec {
  private ByteArrayOutputStream outputBuffer;

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(compressBound(data.remaining()));
    OutputStream ios = compressedStream(baos);
    writeAndClose(data, ios);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  /** Best guess for the compression output buffer size
   * given the uncompressed data size **/
  protected int compressBound(int uncompressedSize) {
    return uncompressedSize;
  }

  /** Return an OutputStream that compresses data to the provided output. **/
  protected abstract OutputStream compressedStream(OutputStream output) throws IOException;

  /** Write the data in the provided ByteBuffer to the OutputStream and close the stream.
   * The ByteBuffer must not be direct.
   */
  void writeAndClose(ByteBuffer data, OutputStream to) throws IOException {
    byte[] input = data.array();
    int offset = data.arrayOffset() + data.position();
    int length = data.remaining();
    try {
      to.write(input, offset, length);
    } finally {
      to.close();
    }
  }

  /** Initializes the outputBuffer to the suggestedLength if it has not
   * yet been initialized.  Otherwise, returns the existing buffer.
   */
  protected final ByteArrayOutputStream getOutputBuffer(int suggestedLength) {
    if (null == outputBuffer) {
      outputBuffer = new ByteArrayOutputStream(suggestedLength);
    }
    outputBuffer.reset();
    return outputBuffer;
  }
}
