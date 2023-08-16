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

package org.apache.avro.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class TestAllCodecs {

  @ParameterizedTest
  @MethodSource("codecTypes")
  void codec(String codec, Class<? extends Codec> codecClass) throws IOException {
    int inputSize = 500_000;

    byte[] input = generateTestData(inputSize);

    Codec codecInstance = CodecFactory.fromString(codec).createInstance();
    Assertions.assertTrue(codecClass.isInstance(codecInstance));
    Assertions.assertTrue(codecInstance.getName().equals(codec));

    ByteBuffer inputByteBuffer = ByteBuffer.wrap(input);
    ByteBuffer compressedBuffer = codecInstance.compress(inputByteBuffer);

    int compressedSize = compressedBuffer.remaining();

    // Make sure something returned
    Assertions.assertTrue(compressedSize > 0);

    // While the compressed size could in many real cases
    // *increase* compared to the input size, our input data
    // is extremely easy to compress and all Avro's compression algorithms
    // should have a compression ratio greater than 1 (except 'null').
    Assertions.assertTrue(compressedSize < inputSize || codec.equals("null"));

    // Decompress the data
    ByteBuffer decompressedBuffer = codecInstance.decompress(compressedBuffer);

    // Validate the the input and output are equal.
    inputByteBuffer.rewind();
    Assertions.assertEquals(inputByteBuffer, decompressedBuffer);
  }

  @ParameterizedTest
  @MethodSource("codecTypes")
  void codecSlice(String codec, Class<? extends Codec> codecClass) throws IOException {
    int inputSize = 500_000;
    byte[] input = generateTestData(inputSize);

    Codec codecInstance = CodecFactory.fromString(codec).createInstance();
    Assertions.assertTrue(codecClass.isInstance(codecInstance));

    ByteBuffer partialBuffer = ByteBuffer.wrap(input);
    partialBuffer.position(17);

    ByteBuffer inputByteBuffer = partialBuffer.slice();
    ByteBuffer compressedBuffer = codecInstance.compress(inputByteBuffer);

    int compressedSize = compressedBuffer.remaining();

    // Make sure something returned
    Assertions.assertTrue(compressedSize > 0);

    // Create a slice from the compressed buffer
    ByteBuffer sliceBuffer = ByteBuffer.allocate(compressedSize + 100);
    sliceBuffer.position(50);
    sliceBuffer.put(compressedBuffer);
    sliceBuffer.limit(compressedSize + 50);
    sliceBuffer.position(50);

    // Decompress the data
    ByteBuffer decompressedBuffer = codecInstance.decompress(sliceBuffer.slice());

    // Validate the the input and output are equal.
    inputByteBuffer.rewind();
    Assertions.assertEquals(inputByteBuffer, decompressedBuffer);
  }

  public static Stream<Arguments> codecTypes() {
    return Stream.of(Arguments.of("bzip2", BZip2Codec.class), Arguments.of("zstandard", ZstandardCodec.class),
        Arguments.of("null", NullCodec.class), Arguments.of("xz", XZCodec.class),
        Arguments.of("snappy", SnappyCodec.class), Arguments.of("deflate", DeflateCodec.class));
  }

  // Generate some test data that will compress easily
  public static byte[] generateTestData(int inputSize) {
    byte[] arr = new byte[inputSize];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (byte) (65 + i % 10);
    }

    return arr;
  }
}
