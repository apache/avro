/**
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
package org.apache.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Assert;
import org.junit.Test;

public class TestDataFileReflect {

  private static final File DIR = new File(System.getProperty("test.dir",
      "/tmp"));
  private static final File FILE = new File(DIR, "test.avro");

  /*
   * Test that using multiple schemas in a file works doing a union before
   * writing any records.
   */
  @Test
  public void testMultiReflectWithUnionBeforeWriting() throws IOException {
    FileOutputStream fos = new FileOutputStream(FILE);

    ReflectData reflectData = ReflectData.get();
    List<Schema> schemas = Arrays.asList(new Schema[] {
        reflectData.getSchema(FooRecord.class),
        reflectData.getSchema(BarRecord.class) });
    Schema union = Schema.createUnion(schemas);
    DataFileWriter<Object> writer =
      new DataFileWriter<Object>(new ReflectDatumWriter(union))
      .create(union, fos);

    // test writing to a file
    CheckList check = new CheckList();
    write(writer, new BarRecord("One beer please"), check);
    write(writer, new FooRecord(10), check);
    write(writer, new BarRecord("Two beers please"), check);
    write(writer, new FooRecord(20), check);
    writer.close();

    ReflectDatumReader din = new ReflectDatumReader();
    SeekableFileInput sin = new SeekableFileInput(FILE);
    DataFileReader<Object> reader = new DataFileReader<Object>(sin, din);
    int count = 0;
    for (Object datum : reader)
      check.assertEquals(datum, count++);
    Assert.assertEquals(count, check.size());
    reader.close();
  }

  /*
   * Test that writing a record with a field that is null.
   */
  @Test
  public void testNull() throws IOException {
    FileOutputStream fos = new FileOutputStream(FILE);

    ReflectData reflectData = ReflectData.AllowNull.get();
    Schema schema = reflectData.getSchema(BarRecord.class);
    DataFileWriter<Object> writer = new DataFileWriter<Object>
      (new ReflectDatumWriter(BarRecord.class, reflectData))
      .create(schema, fos);

    // test writing to a file
    CheckList check = new CheckList();
    write(writer, new BarRecord("One beer please"), check);
    // null record here, fails when using the default reflectData instance
    write(writer, new BarRecord(), check);
    write(writer, new BarRecord("Two beers please"), check);
    writer.close();

    ReflectDatumReader din = new ReflectDatumReader();
    SeekableFileInput sin = new SeekableFileInput(FILE);
    DataFileReader<Object> reader = new DataFileReader<Object>(sin, din);
    int count = 0;
    for (Object datum : reader)
      check.assertEquals(datum, count++);
    Assert.assertEquals(count, check.size());
    reader.close();
  }

  /*
   * Test that writing out and reading in a nested class works
   */
  @Test
  public void testNestedClass() throws IOException {
    FileOutputStream fos = new FileOutputStream(FILE);

    Schema schema = ReflectData.get().getSchema(BazRecord.class);
    DataFileWriter<Object> writer =
      new DataFileWriter<Object>(new ReflectDatumWriter(schema))
      .create(schema, fos);

    // test writing to a file
    CheckList check = new CheckList();
    write(writer, new BazRecord(10), check);
    write(writer, new BazRecord(20), check);
    writer.close();

    ReflectDatumReader din = new ReflectDatumReader();
    SeekableFileInput sin = new SeekableFileInput(FILE);
    DataFileReader<Object> reader = new DataFileReader<Object>(sin, din);
    int count = 0;
    for (Object datum : reader)
      check.assertEquals(datum, count++);
    Assert.assertEquals(count, check.size());
    reader.close();
  }

  private void write(DataFileWriter<Object> writer, Object o, CheckList l)
      throws IOException {
    writer.append(l.addAndReturn(o));
  }

  @SuppressWarnings("serial")
  private static class CheckList extends ArrayList<Object> {
    Object addAndReturn(Object check) {
      add(check);
      return check;
    }

    void assertEquals(Object toCheck, int i) {
      Assert.assertNotNull(toCheck);
      Object o = get(i);
      Assert.assertNotNull(o);
      Assert.assertEquals(toCheck, o);
    }
  }

  private static class BazRecord {
    private int nbr;

    @SuppressWarnings("unused")
    public BazRecord() {
    }

    public BazRecord(int nbr) {
      this.nbr = nbr;
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof BazRecord) {
        return this.nbr == ((BazRecord) that).nbr;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return nbr;
    }

    @Override
    public String toString() {
      return BazRecord.class.getSimpleName() + "{cnt=" + nbr + "}";
    }
  }
}
