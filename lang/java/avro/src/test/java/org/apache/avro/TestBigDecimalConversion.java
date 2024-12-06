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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TestBigDecimalConversion {

  private Conversion<BigDecimal> conversion = new Conversions.BigDecimalConversion();

  private final LogicalType bigDecimal = LogicalTypes.bigDecimal();

  private Schema bytesSchema = conversion.getRecommendedSchema();

  @ParameterizedTest
  @MethodSource("listBigDecimal")
  void bigdec(BigDecimal d1) {
    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, bigDecimal);
    BigDecimal decimal1 = conversion.fromBytes(d1bytes, bytesSchema, bigDecimal);
    Assertions.assertEquals(decimal1, d1);
  }

  static Stream<Arguments> listBigDecimal() {
    Iterator<BigDecimal> iterator = new Iterator<BigDecimal>() {
      int index = 0;

      BigDecimal step = new BigDecimal(-2.7d);

      BigDecimal current = new BigDecimal(1.0d);

      @Override
      public boolean hasNext() {
        if (index == 50) {
          // test small bigdecimal
          current = new BigDecimal(1.0d);
          step = new BigDecimal(-0.71d);
        }
        return index < 100;
      }

      @Override
      public BigDecimal next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        index++;
        current = current.multiply(step);
        return current;
      }
    };
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .map(Arguments::of);

  }

}
