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
package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.specific.TestRecordWithMapsAndArrays;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestBlockingDirectBinaryEncoder {

  private void writeToArray(BinaryEncoder encoder, int[] numbers) throws IOException {
    encoder.writeArrayStart();
    encoder.setItemCount(numbers.length);
    for (int number : numbers) {
      encoder.startItem();
      encoder.writeString(Integer.toString(number));
    }
    encoder.writeArrayEnd();
  }

  private void writeToMap(BinaryEncoder encoder, long[] numbers) throws IOException {
    encoder.writeMapStart();
    encoder.setItemCount(numbers.length);
    for (long number : numbers) {
      encoder.startItem();
      encoder.writeString(Long.toString(number));
      encoder.writeLong(number);
    }
    encoder.writeMapEnd();
  }

  @Test
  void blockingDirectBinaryEncoder() throws IOException, NoSuchAlgorithmException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().blockingDirectBinaryEncoder(baos, null);

    // This is needed because there is no BlockingDirectBinaryEncoder
    // BinaryMessageWriter
    // available out of the box
    encoder.writeFixed(new byte[] { (byte) 0xC3, (byte) 0x01 });
    encoder.writeFixed(SchemaNormalization.parsingFingerprint("CRC-64-AVRO", TestRecordWithMapsAndArrays.SCHEMA$));

    // Array
    this.writeToArray(encoder, new int[] { 1, 2, 3, 4, 5 });

    // Map
    writeToMap(encoder, new long[] { 1L, 2L, 3L, 4L, 5L });

    // Nested Array

    encoder.writeArrayStart();
    encoder.setItemCount(2);
    this.writeToArray(encoder, new int[] { 1, 2 });
    this.writeToArray(encoder, new int[] { 3, 4, 5 });
    encoder.writeArrayEnd();

    // Nested Map

    encoder.writeMapStart();
    encoder.setItemCount(2);
    encoder.writeString("first");
    this.writeToMap(encoder, new long[] { 1L, 2L });
    encoder.writeString("second");
    this.writeToMap(encoder, new long[] { 3L, 4L, 5L });
    encoder.writeMapEnd();

    // Read

    encoder.flush();

    BinaryMessageDecoder<TestRecordWithMapsAndArrays> decoder = TestRecordWithMapsAndArrays.getDecoder();
    TestRecordWithMapsAndArrays r = decoder.decode(baos.toByteArray());

    assertThat(r.getArr(), is(Arrays.asList("1", "2", "3", "4", "5")));
    Map<String, Long> map = r.getMap();
    assertThat(map.size(), is(5));
    for (long i = 1; i <= 5; i++) {
      assertThat(map.get(Long.toString(i)), is(i));
    }

    assertThat(r.getNestedArr(), is(Arrays.asList(Arrays.asList("1", "2"), Arrays.asList("3", "4", "5"))));

    Map<String, Map<String, Long>> nestedMap = r.getNestedMap();
    assertThat(nestedMap.size(), is(2));

    assertThat(nestedMap.get("first").size(), is(2));
    assertThat(nestedMap.get("first").get("1"), is(1L));
    assertThat(nestedMap.get("first").get("2"), is(2L));

    assertThat(nestedMap.get("second").size(), is(3));
    assertThat(nestedMap.get("second").get("3"), is(3L));
    assertThat(nestedMap.get("second").get("4"), is(4L));
    assertThat(nestedMap.get("second").get("5"), is(5L));
  }

  @Test
  void testSkippingUsingBlocks() throws IOException, NoSuchAlgorithmException {
    // Create an empty schema for read, so we skip over all the fields
    Schema emptySchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"TestRecordWithMapsAndArrays\",\"namespace\":\"org.apache.avro.specific\",\"fields\":[]}");

    GenericDatumReader<?> in = new GenericDatumReader<>(TestRecordWithMapsAndArrays.SCHEMA$, emptySchema);
    Decoder mockDecoder = mock(BinaryDecoder.class);

    for (long i = 0; i < 1; i++) {
      in.read(null, mockDecoder);
    }

    verify(mockDecoder, times(2)).skipMap();
    verify(mockDecoder, times(2)).skipArray();
    verify(mockDecoder, times(0)).readString();
    verify(mockDecoder, times(0)).readLong();
  }
}
