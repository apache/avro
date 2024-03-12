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
package org.apache.avro.mapred;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestAvroOutputFormat {
  @Test
  void setSyncInterval() {
    JobConf jobConf = new JobConf();
    int newSyncInterval = 100000;
    AvroOutputFormat.setSyncInterval(jobConf, newSyncInterval);

    assertEquals(newSyncInterval, jobConf.getInt(AvroOutputFormat.SYNC_INTERVAL_KEY, -1));
  }

  @Test
  void noCodec() {
    JobConf job = new JobConf();
    assertNull(AvroOutputFormat.getCodecFactory(job));

    job = new JobConf();
    job.set("mapred.output.compress", "false");
    job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
    assertNull(AvroOutputFormat.getCodecFactory(job));

    job = new JobConf();
    job.set("mapred.output.compress", "false");
    job.set(AvroJob.OUTPUT_CODEC, "bzip2");
    assertNull(AvroOutputFormat.getCodecFactory(job));
  }

  @Test
  void bZip2CodecUsingHadoopClass() {
    CodecFactory avroBZip2Codec = CodecFactory.fromString("bzip2");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroBZip2Codec.getClass());
  }

  @Test
  void bZip2CodecUsingAvroCodec() {
    CodecFactory avroBZip2Codec = CodecFactory.fromString("bzip2");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set(AvroJob.OUTPUT_CODEC, "bzip2");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroBZip2Codec.getClass());
  }

  @Test
  void deflateCodecUsingHadoopClass() {
    CodecFactory avroDeflateCodec = CodecFactory.fromString("deflate");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DeflateCodec");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroDeflateCodec.getClass());
  }

  @Test
  void deflateCodecUsingAvroCodec() {
    CodecFactory avroDeflateCodec = CodecFactory.fromString("deflate");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set(AvroJob.OUTPUT_CODEC, "deflate");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroDeflateCodec.getClass());
  }

  @Test
  void snappyCodecUsingHadoopClass() {
    CodecFactory avroSnappyCodec = CodecFactory.fromString("snappy");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroSnappyCodec.getClass());
  }

  @Test
  void snappyCodecUsingAvroCodec() {
    CodecFactory avroSnappyCodec = CodecFactory.fromString("snappy");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set(AvroJob.OUTPUT_CODEC, "snappy");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroSnappyCodec.getClass());
  }

  @Test
  void gZipCodecUsingHadoopClass() {
    CodecFactory avroDeflateCodec = CodecFactory.fromString("deflate");

    JobConf job = new JobConf();
    job.set("mapred.output.compress", "true");
    job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GZipCodec");
    CodecFactory factory = AvroOutputFormat.getCodecFactory(job);
    assertNotNull(factory);
    assertEquals(factory.getClass(), avroDeflateCodec.getClass());
  }
}
