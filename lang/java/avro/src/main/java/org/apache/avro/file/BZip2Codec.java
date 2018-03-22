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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/** * Implements bzip2 compression and decompression. */
public class BZip2Codec extends OutputInputStreamCodec {

  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return new BZip2Codec();
    }
  }

  @Override
  public String getName() { return DataFileConstants.BZIP2_CODEC; }

  @Override
  protected OutputStream compressedStream(OutputStream output)
      throws IOException {
    return new BZip2CompressorOutputStream(output);
  }

  @Override
  protected InputStream uncompressedStream(InputStream input) throws IOException {
    return new BZip2CompressorInputStream(input);
  }

  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object other) {
    return (this == other) || (this.getClass() == other.getClass());
  }
}
