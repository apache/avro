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

    List<Schema> schemas = Arrays.asList(new Schema[] {
        ReflectData.getSchema(FooRecord.class),
        ReflectData.getSchema(BarRecord.class) });
    Schema union = Schema.createUnion(schemas);
    DataFileWriter<Object> writer = new DataFileWriter<Object>(union, fos,
        new ReflectDatumWriter(union));

    // test writing to a file
    CheckList check = new CheckList();
    write(writer, new BarRecord("One beer please"), check);
    write(writer, new FooRecord(10), check);
    write(writer, new BarRecord("Two beers please"), check);
    write(writer, new FooRecord(20), check);
    writer.close();

    ReflectDatumReader din = new ReflectDatumReader("org.apache.avro.");
    SeekableFileInput sin = new SeekableFileInput(FILE);
    DataFileReader<Object> reader = new DataFileReader<Object>(sin, din);
    Object datum = null;
    long count = reader.getMetaLong("count");
    for (int i = 0; i < count; i++) {
      datum = reader.next(datum);
      check.assertEquals(datum, i);
    }
    reader.close();
  }

  /*
   * Test that using multiple schemas in a file works doing a union for new
   * types as they come.
   */
  @Test
  public void testMultiReflectWithUntionAfterWriting() throws IOException {
    FileOutputStream fos = new FileOutputStream(FILE);

    List<Schema> schemas = new ArrayList<Schema>();
    schemas.add(ReflectData.getSchema(FooRecord.class));
    Schema union = Schema.createUnion(schemas);
    DataFileWriter<Object> writer = new DataFileWriter<Object>(union, fos,
        new ReflectDatumWriter(union));

    CheckList check = new CheckList();
    // write known type
    write(writer, new FooRecord(10), check);
    write(writer, new FooRecord(15), check);

    // we have a new type, add it to the file
    writer.addSchema(ReflectData.getSchema(BarRecord.class));

    // test writing those new types to a file
    write(writer, new BarRecord("One beer please"), check);
    write(writer, new BarRecord("Two beers please"), check);

    // does foo record still work?
    write(writer, new FooRecord(20), check);

    // get one more bar in, just for laughs
    write(writer, new BarRecord("Many beers please"), check);

    writer.close();

    ReflectDatumReader din = new ReflectDatumReader("org.apache.avro.");
    SeekableFileInput sin = new SeekableFileInput(FILE);
    DataFileReader<Object> reader = new DataFileReader<Object>(sin, din);
    Object datum = null;
    long count = reader.getMetaLong("count");
    for (int i = 0; i < count; i++) {
      datum = reader.next(datum);
      check.assertEquals(datum, i);
    }
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
}
