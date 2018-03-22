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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

/**
 * Implements zstd (https://github.com/facebook/zstd) compression
 * using the jni wrappers from https://github.com/luben/zstd-jni.
 * <p/>
 * Supports compression levels from 1 to 22, with level 1 similar
 * to gzip/deflate in compression ratio, but significantly faster.
 * High levels of compression get close to xz compression ratios
 * with decompression speeds 10x faster than xz.
 */
public class ZstandardCodec extends OutputInputStreamCodec {

  static class Option extends CodecFactory {
    private final int compressionLevel;

    Option(int compressionLevel) {
      this.compressionLevel = compressionLevel;
    }

    @Override
    protected Codec createInstance() {
      return new ZstandardCodec(compressionLevel);
    }
  }

  private final int compressionLevel;

  public ZstandardCodec(int compressionLevel) {
    this.compressionLevel = Math.max(Math.min(compressionLevel, 22), 1);
  }

  @Override
  public String getName() {
    return DataFileConstants.ZSTD_CODEC;
  }

  @Override
  protected int compressBound(int uncompressedSize) {
    return (int) Zstd.compressBound(uncompressedSize);
  }

  @Override
  protected OutputStream compressedStream(OutputStream output)
      throws IOException {
    return new ZstdOutputStream(output, compressionLevel);
  }

  @Override
  protected InputStream uncompressedStream(InputStream input)
      throws IOException {
    return new ZstdInputStream(input);
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
