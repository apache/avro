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
package org.apache.avro;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java harness for the shared cross-SDK "must-reject" binary decoding vectors
 * (AVRO-4304). It loads {@code share/test/data/binary-rejections.json} and
 * asserts that every vector is rejected by the Avro binary decoder with a
 * bounded, well-defined error (a {@link Throwable} that is an
 * {@link Exception}, i.e. not a
 * {@link StackOverflowError}/{@link OutOfMemoryError} crash), for both the
 * classic and the fast reader paths.
 * <p>
 * The shared fixtures guarantee that all language SDKs reject the same
 * malformed inputs identically and do not drift.
 */
public class TestBinaryDecodingRejections {

  private static final String RESOURCE = "/share/test/data/binary-rejections.json";

  static Stream<Arguments> vectors() throws IOException {
    List<Arguments> args = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    try (InputStream in = TestBinaryDecodingRejections.class.getResourceAsStream(RESOURCE)) {
      if (in == null) {
        throw new IOException("Missing shared reject-vector fixture on classpath: " + RESOURCE);
      }
      JsonNode root = mapper.readTree(in);
      for (JsonNode v : root.get("vectors")) {
        args.add(Arguments.of(v.get("name").asText(), v.get("schema").asText(), v.get("category").asText(),
            v.get("bytesHex").asText()));
      }
    }
    if (args.isEmpty()) {
      throw new IOException("No reject vectors found in " + RESOURCE);
    }
    return args.stream();
  }

  private static byte[] fromHex(String hex) {
    int len = hex.length();
    byte[] out = new byte[len / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }

  private static void decode(String schemaJson, byte[] bytes, boolean fastReader) throws IOException {
    Schema schema = new Schema.Parser().parse(schemaJson);
    GenericData data = new GenericData();
    data.setFastReaderEnabled(fastReader);
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema, schema, data);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    reader.read(null, decoder);
  }

  @ParameterizedTest(name = "[{2}] {0}")
  @MethodSource("vectors")
  void vectorIsRejectedByBothReaderPaths(String name, String schemaJson, String category, String bytesHex) {
    byte[] bytes = fromHex(bytesHex);
    for (boolean fastReader : new boolean[] { false, true }) {
      // assertThrows(Exception.class, ...) fails if either nothing is thrown (the
      // malformed input was wrongly accepted) or an Error is thrown (a crash such
      // as StackOverflowError/OutOfMemoryError). Both outcomes are what the
      // hardening must prevent, so a plain bounded Exception is the pass condition.
      try {
        assertThrows(Exception.class, () -> decode(schemaJson, bytes, fastReader),
            () -> "Vector '" + name + "' (" + category + ") was not rejected (fastReader=" + fastReader + ")");
      } catch (AssertionError e) {
        fail(e.getMessage());
      }
    }
  }
}
