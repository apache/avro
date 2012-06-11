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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Abstract base class for output formats that write Avro container files.
 *
 * @param <K> The type of key to write.
 * @param <V> The type of value to write.
 */
public abstract class AvroOutputFormatBase<K, V> extends FileOutputFormat<K, V> {
  /**
   * Gets the configured compression codec from the task context.
   *
   * @param context The task attempt context.
   * @return The compression codec to use for the output Avro container file.
   */
  protected static CodecFactory getCompressionCodec(TaskAttemptContext context) {
    if (FileOutputFormat.getCompressOutput(context)) {
      // Deflate compression.
      int compressionLevel = context.getConfiguration().getInt(
          org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
          org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
      return CodecFactory.deflateCodec(compressionLevel);
    }

    // No compression.
    return CodecFactory.nullCodec();
  }

  /**
   * Gets the target output stream where the Avro container file should be written.
   *
   * @param context The task attempt context.
   * @return The target output stream.
   */
  protected OutputStream getAvroFileOutputStream(TaskAttemptContext context) throws IOException {
    Path path = getDefaultWorkFile(context, org.apache.avro.mapred.AvroOutputFormat.EXT);
    return path.getFileSystem(context.getConfiguration()).create(path);
  }
}
