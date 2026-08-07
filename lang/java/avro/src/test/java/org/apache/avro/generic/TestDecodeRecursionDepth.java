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
package org.apache.avro.generic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SystemLimitException;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.FastReaderBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests for AVRO-4302: decoding a deeply nested payload for a
 * recursive schema must fail with a bounded {@link SystemLimitException} rather
 * than crashing the thread with a {@link StackOverflowError}.
 */
public class TestDecodeRecursionDepth {

  /** A self-referencing linked-list schema: the classic recursion-bomb shape. */
  private static final Schema NODE = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"Node\",\"fields\":[" + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}");

  /**
   * Builds a binary-encoded {@code Node} linked list nested {@code depth} levels
   * deep. Each level selects the {@code Node} union branch (index 1); the final
   * level selects {@code null} (index 0) to terminate. Roughly one byte per
   * level, so a tiny payload encodes enormous nesting.
   */
  private static byte[] linkedList(int depth) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    for (int i = 0; i < depth; i++) {
      encoder.writeIndex(1); // select "Node": one more level of nesting
    }
    encoder.writeIndex(0); // select "null": terminate the list
    encoder.flush();
    return out.toByteArray();
  }

  private static Object read(byte[] bytes, boolean fastReader) throws IOException {
    GenericData data = new GenericData();
    data.setFastReaderEnabled(fastReader);
    GenericDatumReader<Object> reader = new GenericDatumReader<>(NODE, NODE, data);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    return reader.read(null, decoder);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void deeplyNestedInputIsRejectedWithBoundedError(boolean fastReader) throws IOException {
    // ~100k levels: far beyond the default depth limit and enough to overflow the
    // stack if it were left unbounded, yet only ~100kB of input.
    byte[] bomb = linkedList(100_000);
    assertThrows(SystemLimitException.class, () -> read(bomb, fastReader));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void moderatelyNestedInputWithinLimitStillDecodes(boolean fastReader) throws IOException {
    // A legitimately nested value comfortably within the limit must still decode.
    // Two structural descents (union + record) are counted per list level, so
    // keep the level count well under half the default limit.
    Object result = read(linkedList(20), fastReader);
    assertNotNull(result);
  }

  @Test
  void deeplyNestedInputIsRejectedWhenSkipped() throws IOException {
    // The skip path (a writer-only field during resolution, the fast reader's skip
    // steps) descends recursively too, so it must be bounded as well.
    byte[] bomb = linkedList(100_000);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bomb, null);
    assertThrows(SystemLimitException.class, () -> GenericDatumReader.skip(NODE, decoder));
  }

  @Test
  void deeplyNestedInputIsRejectedWhenCompared() throws IOException {
    // BinaryData.compare descends recursively over the schema and must not
    // overflow the stack on a deeply nested recursive value either.
    byte[] bomb = linkedList(100_000);
    assertThrows(SystemLimitException.class, () -> BinaryData.compare(bomb, 0, bomb, 0, NODE));
  }

  @Test
  void standaloneFastReaderTopLevelArrayBoundsDepth() throws IOException {
    // When the fast reader is used standalone with a top-level array, the array
    // reader is the outermost scope: it must open the collection scope (which
    // resets stale depth) before counting its own level, so depth accounting stays
    // consistent and a deeply nested element is still rejected.
    Schema arrayOfNode = Schema.createArray(NODE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(1);
    encoder.startItem();
    for (int i = 0; i < 100_000; i++) {
      encoder.writeIndex(1); // one more nested Node
    }
    encoder.writeIndex(0); // terminate the linked list
    encoder.writeArrayEnd();
    encoder.flush();

    DatumReader<Object> reader = FastReaderBuilder.get().createDatumReader(arrayOfNode);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    assertThrows(SystemLimitException.class, () -> reader.read(null, decoder));
  }
}
