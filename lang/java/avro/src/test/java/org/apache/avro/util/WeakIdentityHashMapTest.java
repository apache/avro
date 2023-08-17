/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.avro.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * This test aims to stress WeakIdentityHashMap class in multithread env.
 */
class WeakIdentityHashMapTest {

  private static final int TEST_SIZE = 4001;

  List<String> data = new ArrayList<>(TEST_SIZE);

  final WeakIdentityHashMap<String, String> map = new WeakIdentityHashMap<>();

  List<RuntimeException> exceptions = new ArrayList<>(TEST_SIZE);

  @Test
  void stressMap() {

    for (int i = 1; i <= TEST_SIZE; i++) {
      data.add("Data_" + i);
    }

    List<Thread> threads = new ArrayList<>(80);
    for (int i = 0; i <= 80; i++) {
      final int seed = (i + 1) * 100;
      Runnable runnable = () -> rundata(seed);
      Thread t = new Thread(runnable);
      threads.add(t);
    }
    threads.forEach(Thread::start);
    threads.forEach((Thread t) -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    Assertions.assertTrue(exceptions.isEmpty());
  }

  void rundata(int seed) {
    try {
      for (int i = 1; i <= TEST_SIZE; i++) {
        String keyValue = data.get((i + seed) % TEST_SIZE);
        map.put(keyValue, keyValue);
        if (i % 200 == 0) {
          sleep();
        }
        String keyValueRemove = data.get(((i + seed) * 3) % TEST_SIZE);
        map.remove(keyValueRemove);
      }
    } catch (RuntimeException ex) {
      exceptions.add(ex);
    }
  }

  void sleep() {
    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
