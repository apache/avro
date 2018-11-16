/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import org.apache.avro.AvroRuntimeException;

/**  Encapsulates the ability to specify and configure a compression codec.
 *
 * Currently there are three codecs registered by default:
 * <ul>
 *   <li>{@code null}</li>
 *   <li>{@code deflate}</li>
 *   <li>{@code snappy}</li>
 *   <li>{@code bzip2}</li>
 * </ul>
 *
 * New and custom codecs can be registered using {@link #addCodec(String,
 * CodecFactory)}.
 */
public abstract class CodecFactory {
  /** Null codec, for no compression. */
  public static CodecFactory nullCodec() {
    return NullCodec.OPTION;
  }

  /** Deflate codec, with specific compression.
   * compressionLevel should be between 1 and 9, inclusive. */
  public static CodecFactory deflateCodec(int compressionLevel) {
    return new DeflateCodec.Option(compressionLevel);
  }

  /** XZ codec, with specific compression.
   * compressionLevel should be between 1 and 9, inclusive. */
  public static CodecFactory xzCodec(int compressionLevel) {
      return new XZCodec.Option(compressionLevel);
  }

  /** Snappy codec.*/
  public static CodecFactory snappyCodec() {
    try {
      return new SnappyCodec.Option();
    } catch (Throwable t) {
      //snappy not available
      return null;
    }
  }

  /** bzip2 codec.*/
  public static CodecFactory bzip2Codec() {
    return new BZip2Codec.Option();
  }

  /** zstandard codec.*/
  public static CodecFactory zstandardCodec() {
    return new ZstandardCodec.Option();
  }

  /** Creates internal Codec. */
  protected abstract Codec createInstance();

  /** Mapping of string names (stored as metas) and codecs.
   * Note that currently options (like compression level)
   * are not recoverable. */
  private static final Map<String, CodecFactory> REGISTERED =
    new HashMap<>();

  public static final int DEFAULT_DEFLATE_LEVEL = Deflater.DEFAULT_COMPRESSION;
  public static final int DEFAULT_XZ_LEVEL = XZCodec.DEFAULT_COMPRESSION;

  static {
    addCodec(DataFileConstants.NULL_CODEC, nullCodec());
    addCodec(DataFileConstants.DEFLATE_CODEC, deflateCodec(DEFAULT_DEFLATE_LEVEL));
    addCodec(DataFileConstants.BZIP2_CODEC, bzip2Codec());
    addCodec(DataFileConstants.XZ_CODEC, xzCodec(DEFAULT_XZ_LEVEL));
    addCodec(DataFileConstants.ZSTANDARD_CODEC, zstandardCodec());
    addCodec(DataFileConstants.SNAPPY_CODEC, snappyCodec());
  }

  /** Maps a codec name into a CodecFactory.
   *
   * Currently there are six codecs registered by default:
   * <ul>
   *   <li>{@code null}</li>
   *   <li>{@code deflate}</li>
   *   <li>{@code snappy}</li>
   *   <li>{@code bzip2}</li>
   *   <li>{@code xz}</li>
   *   <li>{@code zstandard}</li>
   * </ul>
   */
  public static CodecFactory fromString(String s) {
    CodecFactory o = REGISTERED.get(s);
    if (o == null) {
      throw new AvroRuntimeException("Unrecognized codec: " + s);
    }
    return o;
  }



  /** Adds a new codec implementation.  If name already had
   * a codec associated with it, returns the previous codec. */
  public static CodecFactory addCodec(String name, CodecFactory c) {
    if (c != null) {
      return REGISTERED.put(name, c);
    }
    return null;
  }

  @Override
  public String toString() {
    Codec instance = this.createInstance();
    return instance.toString();
  }

}
