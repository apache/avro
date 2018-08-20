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
package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.Decoder;

public class MapReader<K, V> implements FieldReader<Map<K, V>> {

  private FieldReader<K> keyReader;
  private FieldReader<V> valueReader;

  public MapReader(FieldReader<K> keyReader, FieldReader<V> valueReader) {
    this.keyReader = keyReader;
    this.valueReader = valueReader;
  }

  @Override
  public Map<K, V> read(Map<K, V> reuse, Decoder decoder) throws IOException {
    long l = decoder.readMapStart();
    Map<K, V> targetMap = new HashMap<>();

    while (l > 0) {
      for (int i = 0; i < l; i++) {
        K key = keyReader.read(null, decoder);
        V value = valueReader.read(null, decoder);
        targetMap.put(key, value);
      }
      l = decoder.mapNext();
    }

    return targetMap;
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    long l = decoder.readMapStart();
    while (l > 0) {
      for (int i = 0; i < l; i++) {
        keyReader.skip(decoder);
        valueReader.skip(decoder);
      }
      l = decoder.mapNext();
    }
  }
}
