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
package org.apache.avro.util;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class MapUtil {

  private MapUtil() {
    super();
  }

  /**
   * A temporary workaround for Java 8 specific performance issue JDK-8161372
   * .<br>
   * This class should be removed once we drop Java 8 support.
   *
   * @see <a href=
   *      "https://bugs.openjdk.java.net/browse/JDK-8161372">JDK-8161372</a>
   */
  public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Function<K, V> mappingFunction) {
    V value = map.get(key);
    if (value != null) {
      return value;
    }
    return map.computeIfAbsent(key, mappingFunction::apply);
  }

}
