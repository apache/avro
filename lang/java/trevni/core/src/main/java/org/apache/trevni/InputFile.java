/*
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
package org.apache.trevni;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.IOException;

/** An {@link Input} for files. */
public class InputFile implements Input {

  private FileChannel channel;

  /** Construct for the given file. */
  public InputFile(File file) throws IOException {
    this.channel = new FileInputStream(file).getChannel();
  }

  @Override
  public long length() throws IOException { return channel.size(); }

  @Override
  public int read(long position, byte[] b, int start, int len)
    throws IOException {
    return channel.read(ByteBuffer.wrap(b, start, len), position);
  }

  @Override
  public void close() throws IOException { channel.close(); }

}

