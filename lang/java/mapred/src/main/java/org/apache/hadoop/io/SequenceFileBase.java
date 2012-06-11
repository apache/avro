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

package org.apache.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;

/** Base class to permit subclassing SequenceFile.  Provides access to two
 * package-protected classes within SequenceFile. */
public abstract class SequenceFileBase {
  protected SequenceFileBase() {}

  protected abstract static class BlockCompressWriterBase
    extends SequenceFile.BlockCompressWriter {
    protected BlockCompressWriterBase(FileSystem fs, Configuration conf,
                                      Path name,
                                      Class keyClass, Class valClass,
                                      int bufferSize, short replication,
                                      long blockSize, CompressionCodec codec,
                                      Progressable progress, Metadata metadata)
      throws IOException {
      super(fs, conf, name, keyClass, valClass, bufferSize, replication,
            blockSize, codec, progress, metadata);
    }
  }

  protected abstract static class RecordCompressWriterBase
    extends SequenceFile.RecordCompressWriter {
    protected RecordCompressWriterBase(FileSystem fs, Configuration conf,
                                       Path name,
                                       Class keyClass, Class valClass,
                                       int bufferSize, short replication,
                                       long blockSize, CompressionCodec codec,
                                       Progressable progress, Metadata metadata)
      throws IOException {
      super(fs, conf, name, keyClass, valClass, bufferSize, replication,
            blockSize, codec, progress, metadata);
    }
  }

}
