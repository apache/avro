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
package org.apache.avro.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

/** * Implements xz compression and decompression. */
public class XZCodec extends Codec {

  static class Option extends CodecFactory {
      private int compressionLevel;

      Option(int compressionLevel) {
        this.compressionLevel = compressionLevel;
      }

      @Override
      protected Codec createInstance() {
        return new XZCodec(compressionLevel);
      }
    }

  private ByteArrayOutputStream outputBuffer;
  private int compressionLevel;

  public XZCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  public String getName() {
    return DataFileConstants.XZ_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    OutputStream ios = new XZCompressorOutputStream(baos, compressionLevel);
    writeAndClose(data, ios);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    InputStream bytesIn = new ByteArrayInputStream(
      data.array(),
      data.arrayOffset() + data.position(),
      data.remaining());
    InputStream ios = new XZCompressorInputStream(bytesIn);
    try {
      IOUtils.copy(ios, baos);
    } finally {
      ios.close();
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private void writeAndClose(ByteBuffer data, OutputStream to) throws IOException {
    byte[] input = data.array();
    int offset = data.arrayOffset() + data.position();
    int length = data.remaining();
    try {
      to.write(input, offset, length);
    } finally {
      to.close();
    }
  }

  // get and initialize the output buffer for use.
  private ByteArrayOutputStream getOutputBuffer(int suggestedLength) {
    if (null == outputBuffer) {
      outputBuffer = new ByteArrayOutputStream(suggestedLength);
    }
    outputBuffer.reset();
    return outputBuffer;
  }

  @Override
  public int hashCode() {
    return compressionLevel;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    XZCodec other = (XZCodec)obj;
    return (this.compressionLevel == other.compressionLevel);
  }

  @Override
  public String toString() {
    return getName() + "-" + compressionLevel;
  }
}
