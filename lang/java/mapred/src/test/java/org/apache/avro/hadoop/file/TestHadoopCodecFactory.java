/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.hadoop.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.avro.file.CodecFactory;
import org.junit.jupiter.api.Test;

public class TestHadoopCodecFactory {

  @Test
  void hadoopCodecFactoryDeflate() {
    CodecFactory hadoopDeflateCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.DeflateCodec");
    CodecFactory avroDeflateCodec = CodecFactory.fromString("deflate");
    assertEquals(hadoopDeflateCodec.getClass(), avroDeflateCodec.getClass());
  }

  @Test
  void hadoopCodecFactorySnappy() {
    CodecFactory hadoopSnappyCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.SnappyCodec");
    CodecFactory avroSnappyCodec = CodecFactory.fromString("snappy");
    assertEquals(hadoopSnappyCodec.getClass(), avroSnappyCodec.getClass());
  }

  @Test
  void hadoopCodecFactoryBZip2() {
    CodecFactory hadoopSnappyCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.BZip2Codec");
    CodecFactory avroSnappyCodec = CodecFactory.fromString("bzip2");
    assertEquals(hadoopSnappyCodec.getClass(), avroSnappyCodec.getClass());
  }

  @Test
  void hadoopCodecFactoryGZip() {
    CodecFactory hadoopSnappyCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.GZipCodec");
    CodecFactory avroSnappyCodec = CodecFactory.fromString("deflate");
    assertEquals(hadoopSnappyCodec.getClass(), avroSnappyCodec.getClass());
  }

  @Test
  void hadoopCodecFactoryFail() {
    CodecFactory hadoopSnappyCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.FooCodec");
    assertNull(hadoopSnappyCodec);
  }

  @Test
  void hadoopCodecFactoryZstd() {
    CodecFactory hadoopZstdCodec = HadoopCodecFactory.fromHadoopString("org.apache.hadoop.io.compress.ZStandardCodec");
    CodecFactory avroZstdCodec = CodecFactory.fromString("zstandard");
    assertEquals(hadoopZstdCodec.getClass(), avroZstdCodec.getClass());
  }
}
