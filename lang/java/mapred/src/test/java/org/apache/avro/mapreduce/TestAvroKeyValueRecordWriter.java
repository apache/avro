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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.mapreduce;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroDatumConverterFactory;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class TestAvroKeyValueRecordWriter {
  @Test
  public void testWriteRecords() throws IOException {
    Job job = new Job();
    AvroJob.setOutputValueSchema(job, TextStats.SCHEMA$);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    AvroDatumConverterFactory factory = new AvroDatumConverterFactory(job.getConfiguration());
    AvroDatumConverter<Text, ?> keyConverter = factory.create(Text.class);
    AvroValue<TextStats> avroValue = new AvroValue<TextStats>(null);
    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroValue<TextStats>, ?> valueConverter
        = factory.create((Class<AvroValue<TextStats>>) avroValue.getClass());
    CodecFactory compressionCodec = CodecFactory.nullCodec();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // Use a writer to generate a Avro container file in memory.
    // Write two records: <'apple', TextStats('apple')> and <'banana', TextStats('banana')>.
    AvroKeyValueRecordWriter<Text, AvroValue<TextStats>> writer
        = new AvroKeyValueRecordWriter<Text, AvroValue<TextStats>>(keyConverter, valueConverter,
            compressionCodec, outputStream);
    TextStats appleStats = new TextStats();
    appleStats.name = "apple";
    writer.write(new Text("apple"), new AvroValue<TextStats>(appleStats));
    TextStats bananaStats = new TextStats();
    bananaStats.name = "banana";
    writer.write(new Text("banana"), new AvroValue<TextStats>(bananaStats));
    writer.close(context);

    verify(context);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = AvroKeyValue.getSchema(
        Schema.create(Schema.Type.STRING), TextStats.SCHEMA$);
    DatumReader<GenericRecord> datumReader
        = new SpecificDatumReader<GenericRecord>(readerSchema);
    DataFileStream<GenericRecord> avroFileReader
        = new DataFileStream<GenericRecord>(inputStream, datumReader);

    // Verify that the first record was written.
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> firstRecord
        = new AvroKeyValue<CharSequence, TextStats>(avroFileReader.next());
    assertNotNull(firstRecord.get());
    assertEquals("apple", firstRecord.getKey().toString());
    assertEquals("apple", firstRecord.getValue().name.toString());

    // Verify that the second record was written;
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> secondRecord
        = new AvroKeyValue<CharSequence, TextStats>(avroFileReader.next());
    assertNotNull(secondRecord.get());
    assertEquals("banana", secondRecord.getKey().toString());
    assertEquals("banana", secondRecord.getValue().name.toString());

    // That's all, folks.
    assertFalse(avroFileReader.hasNext());
    avroFileReader.close();
  }
}
