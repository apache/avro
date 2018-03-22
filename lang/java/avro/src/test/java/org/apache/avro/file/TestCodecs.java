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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class TestCodecs {

  @Parameters(name = "Codec: {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { new NullCodec() },
      { new DeflateCodec(3) },
      { new BZip2Codec() },
      { new SnappyCodec() },
      { new XZCodec(3) },
      { new ZstandardCodec(3) }
    });
  }

  private final Codec codec;
  private final byte[] zeroes = new byte[1024*1024];
  private final byte[] random = new byte[1024*1024];

  public TestCodecs(Codec codec) {
    this.codec = codec;
  }

  {
    new Random().nextBytes(random);
  }

  @Test
  public void roundTripZeroes() throws IOException {
    roundTrip(zeroes);
  }

  @Test
  public void roundTripRandomData() throws IOException {
    roundTrip(random);
  }

  public void roundTrip(byte[] bytes) throws IOException {
    ByteBuffer compressed = codec.compress(ByteBuffer.wrap(bytes));
    ByteBuffer uncompressed = codec.decompress(compressed);
    Assert.assertArrayEquals(bytes, uncompressed.array());
  }

  @Test
  public void testEquals() {
    Codec fromFactory = CodecFactory.fromString(codec.getName()).createInstance();
    Assert.assertEquals(codec, fromFactory);
    Assert.assertEquals(codec.hashCode(), fromFactory.hashCode());
  }
}
