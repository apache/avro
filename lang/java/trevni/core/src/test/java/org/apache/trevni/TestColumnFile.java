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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.io.File;
import java.util.Random;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized.Parameters;

public class TestColumnFile {

  private static final File FILE = new File("target", "test.trv");
  private static final int COUNT = 1024 * 64;

  @Parameters
  public static Stream<Arguments> codecs() {
    return Stream.of(Arguments.of(createFileMeta("null", "null")), Arguments.of(createFileMeta("snappy", "crc32")),
        Arguments.of(createFileMeta("deflate", "crc32")));
  }

  private static ColumnFileMetaData createFileMeta(String codec, String checksum) {
    return new ColumnFileMetaData().setCodec(codec).setChecksum(checksum);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void emptyFile(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();
    ColumnFileWriter out = new ColumnFileWriter(fileMeta);
    out.writeTo(FILE);
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(0, in.getRowCount());
    Assertions.assertEquals(0, in.getColumnCount());
    in.close();
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void emptyColumn(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();
    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("test", ValueType.INT));
    out.writeTo(FILE);
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(0, in.getRowCount());
    Assertions.assertEquals(1, in.getColumnCount());
    ColumnValues<Integer> values = in.getValues("test");
    for (int i : values)
      throw new Exception("no value should be found");
    in.close();
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void ints(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();

    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("test", ValueType.INT));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(TestUtil.randomLength(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(COUNT, in.getRowCount());
    Assertions.assertEquals(1, in.getColumnCount());
    Iterator<Integer> i = in.getValues("test");
    int count = 0;
    while (i.hasNext()) {
      Assertions.assertEquals(TestUtil.randomLength(random), (int) i.next());
      count++;
    }
    Assertions.assertEquals(COUNT, count);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void longs(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();

    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("test", ValueType.LONG));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(random.nextLong());
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(COUNT, in.getRowCount());
    Assertions.assertEquals(1, in.getColumnCount());
    Iterator<Long> i = in.getValues("test");
    int count = 0;
    while (i.hasNext()) {
      Assertions.assertEquals(random.nextLong(), (long) i.next());
      count++;
    }
    Assertions.assertEquals(COUNT, count);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void strings(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();

    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("test", ValueType.STRING));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(TestUtil.randomString(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(COUNT, in.getRowCount());
    Assertions.assertEquals(1, in.getColumnCount());
    Iterator<String> i = in.getValues("test");
    int count = 0;
    while (i.hasNext()) {
      Assertions.assertEquals(TestUtil.randomString(random), i.next());
      count++;
    }
    Assertions.assertEquals(COUNT, count);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void twoColumn(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();
    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("a", ValueType.FIXED32),
        new ColumnMetaData("b", ValueType.STRING));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(random.nextInt(), TestUtil.randomString(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assertions.assertEquals(COUNT, in.getRowCount());
    Assertions.assertEquals(2, in.getColumnCount());
    Iterator<String> i = in.getValues("a");
    Iterator<String> j = in.getValues("b");
    int count = 0;
    while (i.hasNext() && j.hasNext()) {
      Assertions.assertEquals(random.nextInt(), i.next());
      Assertions.assertEquals(TestUtil.randomString(random), j.next());
      count++;
    }
    Assertions.assertEquals(COUNT, count);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void seekLongs(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();

    ColumnFileWriter out = new ColumnFileWriter(fileMeta, new ColumnMetaData("test", ValueType.LONG));
    Random random = TestUtil.createRandom();

    int seekCount = COUNT / 1024;
    int[] seekRows = new int[seekCount];
    Map<Integer, Integer> seekRowMap = new HashMap<>(seekCount);
    while (seekRowMap.size() < seekCount) {
      int row = random.nextInt(COUNT);
      if (!seekRowMap.containsKey(row)) {
        seekRows[seekRowMap.size()] = row;
        seekRowMap.put(row, seekRowMap.size());
      }
    }

    Long[] seekValues = new Long[seekCount];
    for (int i = 0; i < COUNT; i++) {
      long l = random.nextLong();
      out.writeRow(l);
      if (seekRowMap.containsKey(i))
        seekValues[seekRowMap.get(i)] = l;
    }
    out.writeTo(FILE);

    ColumnFileReader in = new ColumnFileReader(FILE);
    ColumnValues<Long> v = in.getValues("test");

    for (int i = 0; i < seekCount; i++) {
      v.seek(seekRows[i]);
      Assertions.assertEquals(seekValues[i], v.next());
    }

  }

  @ParameterizedTest
  @MethodSource("codecs")
  void seekStrings(ColumnFileMetaData fileMeta) throws Exception {
    FILE.delete();

    ColumnFileWriter out = new ColumnFileWriter(fileMeta,
        new ColumnMetaData("test", ValueType.STRING).hasIndexValues(true));

    Random random = TestUtil.createRandom();

    int seekCount = COUNT / 1024;
    Map<Integer, Integer> seekRowMap = new HashMap<>(seekCount);
    while (seekRowMap.size() < seekCount) {
      int row = random.nextInt(COUNT);
      if (!seekRowMap.containsKey(row))
        seekRowMap.put(row, seekRowMap.size());
    }

    String[] values = new String[COUNT];
    for (int i = 0; i < COUNT; i++)
      values[i] = TestUtil.randomString(random);
    Arrays.sort(values);

    String[] seekValues = new String[seekCount];
    for (int i = 0; i < COUNT; i++) {
      out.writeRow(values[i]);
      if (seekRowMap.containsKey(i))
        seekValues[seekRowMap.get(i)] = values[i];
    }
    out.writeTo(FILE);

    ColumnFileReader in = new ColumnFileReader(FILE);
    ColumnValues<String> v = in.getValues("test");

    for (int i = 0; i < seekCount; i++) {
      v.seek(seekValues[i]);
      Assertions.assertEquals(seekValues[i], v.next());
    }

  }

}
