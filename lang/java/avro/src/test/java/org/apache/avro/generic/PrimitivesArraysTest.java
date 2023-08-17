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

import org.apache.avro.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PrimitivesArraysTest {

  @Test
  void booleanArray() {
    PrimitivesArrays.BooleanArray ba = new PrimitivesArrays.BooleanArray(4,
        Schema.createArray(Schema.create(Schema.Type.BOOLEAN)));

    Assertions.assertEquals(0, ba.size());
    for (int i = 1; i < 100; i++) {
      if (i % 3 == 0 || i % 5 == 0) {
        ba.add(true);
      } else {
        ba.add(false);
      }
    }
    Assertions.assertEquals(99, ba.size());
    for (int i = 1; i < 100; i++) {
      if (i % 3 == 0 || i % 5 == 0) {
        Assertions.assertTrue(ba.get(i - 1), "Error for " + i);
      } else {
        Assertions.assertFalse(ba.get(i - 1), "Error for " + i);
      }
    }
    Assertions.assertFalse(ba.remove(12));
    Assertions.assertEquals(98, ba.size());
    for (int i = 13; i < 99; i++) {
      if ((i + 1) % 3 == 0 || (i + 1) % 5 == 0) {
        Assertions.assertTrue(ba.get(i - 1), "After delete, Error for " + i);
      } else {
        Assertions.assertFalse(ba.get(i - 1), "After delete, Error for " + i);
      }
    }

    ba.add(12, false);
    Assertions.assertEquals(99, ba.size());
    for (int i = 1; i < 100; i++) {
      if (i % 3 == 0 || i % 5 == 0) {
        Assertions.assertTrue(ba.get(i - 1), "Error for " + i);
      } else {
        Assertions.assertFalse(ba.get(i - 1), "Error for " + i);
      }
    }
    Assertions.assertFalse(ba.remove(12));
    ba.add(12, true);
    for (int i = 1; i < 100; i++) {
      if (i % 3 == 0 || i % 5 == 0 || i == 13) {
        Assertions.assertTrue(ba.get(i - 1), "Error for " + i);
      } else {
        Assertions.assertFalse(ba.get(i - 1), "Error for " + i);
      }
    }
    ba.add(99, true);
    Assertions.assertTrue(ba.get(99), "Error for 99");
    ba.remove(99);
    ba.reverse();
    for (int i = 1; i < 100; i++) {
      if (i % 3 == 0 || i % 5 == 0 || i == 13) {
        Assertions.assertTrue(ba.get(99 - i), "Error for " + i);
      } else {
        Assertions.assertFalse(ba.get(99 - i), "Error for " + i);
      }
    }
  }

  @Test
  void booleanArrayIterator() {
    PrimitivesArrays.BooleanArray ba = new PrimitivesArrays.BooleanArray(4,
        Schema.createArray(Schema.create(Schema.Type.BOOLEAN)));
    boolean[] model = new boolean[] { true, false, false, true, true, true, false, false, true, false, false };
    for (boolean x : model) {
      ba.add(x);
    }
    Assertions.assertEquals(model.length, ba.size());
    int index = 0;
    for (Boolean b : ba) {
      Assertions.assertEquals(model[index], b);
      index++;
    }
  }

  @Test
  void intArray() {
    final PrimitivesArrays.IntArray intArray = new PrimitivesArrays.IntArray(4,
        Schema.createArray(Schema.create(Schema.Type.INT)));
    for (int i = 1; i <= 100; i++) {
      intArray.add(i);
    }
    Assertions.assertEquals(100, intArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i, intArray.get(i - 1));
    }

    int expectedValue = 1;
    for (Integer value : intArray) {
      Assertions.assertEquals(expectedValue, value);
      expectedValue++;
    }

    intArray.remove(40);
    Assertions.assertEquals(99, intArray.size());
    for (int i = 1; i <= 99; i++) {
      if (i <= 40) {
        Assertions.assertEquals(i, intArray.get(i - 1));
      } else {
        Assertions.assertEquals(i + 1, intArray.get(i - 1));
      }
    }
    intArray.add(40, 41);
    Assertions.assertEquals(100, intArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i, intArray.get(i - 1));
    }
    intArray.set(40, 25);
    Assertions.assertEquals(25, intArray.get(40));

    Assertions.assertEquals(0, intArray.peek());
    intArray.set(40, 41);
    intArray.reverse();
    Assertions.assertEquals(100, intArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(101 - i, intArray.get(i - 1));
    }
  }

  @Test
  void longArray() {
    final PrimitivesArrays.LongArray longArray = new PrimitivesArrays.LongArray(4,
        Schema.createArray(Schema.create(Schema.Type.LONG)));
    for (long i = 1; i <= 100; i++) {
      longArray.add(i);
    }
    Assertions.assertEquals(100l, longArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i, longArray.get(i - 1));
    }

    int expectedValue = 1;
    for (Long value : longArray) {
      Assertions.assertEquals(expectedValue, value);
      expectedValue++;
    }

    longArray.remove(40);
    Assertions.assertEquals(99, longArray.size());
    for (int i = 1; i <= 99; i++) {
      if (i <= 40) {
        Assertions.assertEquals(i, longArray.get(i - 1));
      } else {
        Assertions.assertEquals(i + 1, longArray.get(i - 1));
      }
    }
    longArray.add(40, 41);
    Assertions.assertEquals(100, longArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i, longArray.get(i - 1));
    }
    longArray.set(40, 25);
    Assertions.assertEquals(25, longArray.get(40));

    Assertions.assertEquals(0, longArray.peek());
    longArray.set(40, 41);
    longArray.reverse();
    Assertions.assertEquals(100, longArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(101 - i, longArray.get(i - 1));
    }
  }

  @Test
  void floatArray() {
    final PrimitivesArrays.FloatArray floatArray = new PrimitivesArrays.FloatArray(4,
        Schema.createArray(Schema.create(Schema.Type.FLOAT)));
    for (int i = 1; i <= 100; i++) {
      floatArray.add(i * 3.3f);
    }
    Assertions.assertEquals(100, floatArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i * 3.3f, floatArray.get(i - 1));
    }

    float expectedValue = 1.0f;
    for (Float value : floatArray) {
      Assertions.assertEquals(expectedValue * 3.3f, value);
      expectedValue++;
    }

    floatArray.remove(40);
    Assertions.assertEquals(99, floatArray.size());
    for (int i = 1; i <= 99; i++) {
      if (i <= 40) {
        Assertions.assertEquals(i * 3.3f, floatArray.get(i - 1));
      } else {
        Assertions.assertEquals((i + 1) * 3.3f, floatArray.get(i - 1));
      }
    }
    floatArray.add(40, 41 * 3.3f);
    Assertions.assertEquals(100, floatArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i * 3.3f, floatArray.get(i - 1));
    }
    floatArray.set(40, 25.2f);
    Assertions.assertEquals(25.2f, floatArray.get(40));

    Assertions.assertEquals(0.0f, floatArray.peek());
    floatArray.set(40, 41 * 3.3f);
    floatArray.reverse();
    Assertions.assertEquals(100, floatArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals((101 - i) * 3.3f, floatArray.get(i - 1));
    }
  }

  @Test
  void doubleArray() {
    final PrimitivesArrays.DoubleArray doubleArray = new PrimitivesArrays.DoubleArray(4,
        Schema.createArray(Schema.create(Schema.Type.DOUBLE)));
    for (int i = 1; i <= 100; i++) {
      doubleArray.add(i * 3.0d);
    }
    Assertions.assertEquals(100, doubleArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i * 3.0d, doubleArray.get(i - 1));
    }

    double expectedValue = 1.0f;
    for (Double value : doubleArray) {
      Assertions.assertEquals(expectedValue * 3.0d, value);
      expectedValue++;
    }

    doubleArray.remove(40);
    Assertions.assertEquals(99, doubleArray.size());
    for (int i = 1; i <= 99; i++) {
      if (i <= 40) {
        Assertions.assertEquals(i * 3.0d, doubleArray.get(i - 1));
      } else {
        Assertions.assertEquals((i + 1) * 3.0d, doubleArray.get(i - 1));
      }
    }
    doubleArray.add(40, 41 * 3.0d);
    Assertions.assertEquals(100, doubleArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals(i * 3.0d, doubleArray.get(i - 1));
    }
    doubleArray.set(40, 25.2d);
    Assertions.assertEquals(25.2d, doubleArray.get(40));

    Assertions.assertEquals(0.0d, doubleArray.peek());
    doubleArray.set(40, 41 * 3.0d);
    doubleArray.reverse();
    Assertions.assertEquals(100, doubleArray.size());
    for (int i = 1; i <= 100; i++) {
      Assertions.assertEquals((101 - i) * 3.0d, doubleArray.get(i - 1));
    }
  }
}
