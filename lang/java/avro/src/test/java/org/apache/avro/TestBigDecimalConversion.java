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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RunWith(Parameterized.class)
public class TestBigDecimalConversion {

  private Conversion<BigDecimal> conversion = new Conversions.BigDecimalConversion();

  private final LogicalType bigDecimal = LogicalTypes.bigDecimal();

  private Schema bytesSchema = conversion.getRecommendedSchema();

  @Parameterized.Parameter(0)
  public BigDecimal decimal;

  @Test
  public void bigdec() {
    ByteBuffer d1bytes = conversion.toBytes(decimal, bytesSchema, bigDecimal);
    BigDecimal decimal1 = conversion.fromBytes(d1bytes, bytesSchema, bigDecimal);
    Assert.assertEquals(decimal1, decimal);
  }

  @Parameters
  public static Collection<Object[]> listBigDecimal() {
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
        .map(t -> new Object[] { t }).collect(Collectors.toList());

  }

}
