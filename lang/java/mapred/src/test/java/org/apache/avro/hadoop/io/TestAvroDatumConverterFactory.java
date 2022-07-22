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

package org.apache.avro.hadoop.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAvroDatumConverterFactory {
  private Job mJob;
  private AvroDatumConverterFactory mFactory;

  @BeforeEach
  public void setup() throws IOException {
    mJob = Job.getInstance();
    mFactory = new AvroDatumConverterFactory(mJob.getConfiguration());
  }

  @Test
  void convertAvroKey() throws IOException {
    AvroJob.setOutputKeySchema(mJob, Schema.create(Schema.Type.STRING));

    AvroKey<CharSequence> avroKey = new AvroKey<>("foo");
    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroKey<CharSequence>, ?> converter = mFactory
        .create((Class<AvroKey<CharSequence>>) avroKey.getClass());
    assertEquals("foo", converter.convert(avroKey).toString());
  }

  @Test
  void convertAvroValue() throws IOException {
    AvroJob.setOutputValueSchema(mJob, Schema.create(Schema.Type.INT));

    AvroValue<Integer> avroValue = new AvroValue<>(42);
    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroValue<Integer>, Integer> converter = mFactory
        .create((Class<AvroValue<Integer>>) avroValue.getClass());
    assertEquals(42, converter.convert(avroValue).intValue());
  }

  @Test
  void convertBooleanWritable() {
    AvroDatumConverter<BooleanWritable, Boolean> converter = mFactory.create(BooleanWritable.class);
    assertEquals(true, converter.convert(new BooleanWritable(true)));
  }

  @Test
  void convertBytesWritable() {
    AvroDatumConverter<BytesWritable, ByteBuffer> converter = mFactory.create(BytesWritable.class);
    ByteBuffer bytes = converter.convert(new BytesWritable(new byte[] { 1, 2, 3 }));
    assertEquals(1, bytes.get(0));
    assertEquals(2, bytes.get(1));
    assertEquals(3, bytes.get(2));
  }

  @Test
  void convertByteWritable() {
    AvroDatumConverter<ByteWritable, GenericFixed> converter = mFactory.create(ByteWritable.class);
    assertEquals(42, converter.convert(new ByteWritable((byte) 42)).bytes()[0]);
  }

  @Test
  void convertDoubleWritable() {
    AvroDatumConverter<DoubleWritable, Double> converter = mFactory.create(DoubleWritable.class);
    assertEquals(2.0, converter.convert(new DoubleWritable(2.0)), 0.00001);
  }

  @Test
  void convertFloatWritable() {
    AvroDatumConverter<FloatWritable, Float> converter = mFactory.create(FloatWritable.class);
    assertEquals(2.2f, converter.convert(new FloatWritable(2.2f)), 0.00001);
  }

  @Test
  void convertIntWritable() {
    AvroDatumConverter<IntWritable, Integer> converter = mFactory.create(IntWritable.class);
    assertEquals(2, converter.convert(new IntWritable(2)).intValue());
  }

  @Test
  void convertLongWritable() {
    AvroDatumConverter<LongWritable, Long> converter = mFactory.create(LongWritable.class);
    assertEquals(123L, converter.convert(new LongWritable(123L)).longValue());
  }

  @Test
  void convertNullWritable() {
    AvroDatumConverter<NullWritable, Object> converter = mFactory.create(NullWritable.class);
    assertNull(converter.convert(NullWritable.get()));
  }

  @Test
  void convertText() {
    AvroDatumConverter<Text, CharSequence> converter = mFactory.create(Text.class);
    assertEquals("foo", converter.convert(new Text("foo")).toString());
  }
}
