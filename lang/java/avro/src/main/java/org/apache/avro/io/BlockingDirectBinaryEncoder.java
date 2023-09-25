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
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link Encoder} for Avro's binary encoding that does not buffer output.
 * <p/>
 * This encoder does not buffer writes, and as a result is slower than
 * {@link BufferedBinaryEncoder}. However, it is lighter-weight and useful when
 * the buffering in BufferedBinaryEncoder is not desired and/or the Encoder is
 * very short-lived.
 * <p/>
 * To construct, use
 * {@link EncoderFactory#directBinaryEncoder(OutputStream, BinaryEncoder)}
 * <p/>
 * DirectBinaryEncoder is not thread-safe
 *
 * @see BinaryEncoder
 * @see EncoderFactory
 * @see Encoder
 * @see Decoder
 */
public class BlockingDirectBinaryEncoder extends DirectBinaryEncoder {
  private OutputStream originalStream;

  private final ByteArrayOutputStream buffer;

  private boolean inBlock = false;

  private long blockItemCount;

  /**
   * Create a writer that sends its output to the underlying stream
   * <code>out</code>.
   *
   * @param out The Outputstream to write to
   */
  public BlockingDirectBinaryEncoder(OutputStream out) {
    super(out);
    buffer = new ByteArrayOutputStream();
  }

  private void startBlock() {
    if (inBlock) {
      throw new RuntimeException("Nested Maps/Arrays are not supported by the BlockingDirectBinaryEncoder");
    }
    originalStream = out;
    out = buffer;
    inBlock = true;
  }

  private void endBlock() {
    if (!inBlock) {
      throw new RuntimeException("Called endBlock, while not buffering a block");
    }
    out = originalStream;
    if (blockItemCount > 0) {
      try {
        // Make it negative, so the reader knows that the number of bytes is coming
        writeLong(-blockItemCount);
        writeLong(buffer.size());
        writeBytes(buffer.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    inBlock = false;
    buffer.reset();
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    blockItemCount = itemCount;
  }

  @Override
  public void writeArrayStart() throws IOException {
    startBlock();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    endBlock();
    // Writes another zero to indicate that this is the last block
    super.writeArrayEnd();
  }

  @Override
  public void writeMapStart() throws IOException {
    startBlock();
  }

  @Override
  public void writeMapEnd() throws IOException {
    endBlock();
    // Writes another zero to indicate that this is the last block
    super.writeMapEnd();
  }
}
