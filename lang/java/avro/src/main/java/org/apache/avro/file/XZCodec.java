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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;

/** * Implements xz compression and decompression. */
public class XZCodec extends OutputInputStreamCodec {

  static class Option extends CodecFactory {
      private final int compressionLevel;

      Option(int compressionLevel) {
        this.compressionLevel = compressionLevel;
      }

      @Override
      protected Codec createInstance() {
        return new XZCodec(compressionLevel);
      }
    }

  private final int compressionLevel;

  public XZCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  public String getName() {
    return DataFileConstants.XZ_CODEC;
  }

  @Override
  protected OutputStream compressedStream(OutputStream output)
      throws IOException {
    return new XZCompressorOutputStream(output, compressionLevel);
  }

  @Override
  protected InputStream uncompressedStream(InputStream input)
      throws IOException {
    return new XZCompressorInputStream(input);
  }

  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object other) {
    return (this == other) || (this.getClass() == other.getClass());
  }

  @Override
  public String toString() {
    return getName() + "-" + compressionLevel;
  }
}
