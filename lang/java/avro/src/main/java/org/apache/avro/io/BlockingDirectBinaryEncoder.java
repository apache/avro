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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;

/**
 * An {@link Encoder} for Avro's binary encoding that does not buffer output.
 * <p/>
 * This encoder does not buffer writes in contrast to
 * {@link BufferedBinaryEncoder}. However, it is lighter-weight and useful when:
 * The buffering in BufferedBinaryEncoder is not desired because you buffer a
 * different level or the Encoder is very short-lived.
 * </p>
 * The BlockingDirectBinaryEncoder will encode the number of bytes of the Map
 * and Array blocks. This will allow to postpone the decoding, or skip over it
 * at all.
 * <p/>
 * To construct, use
 * {@link EncoderFactory#blockingDirectBinaryEncoder(OutputStream, BinaryEncoder)}
 * <p/>
 * {@link BlockingDirectBinaryEncoder} instances returned by this method are not
 * thread-safe
 *
 * @see BinaryEncoder
 * @see EncoderFactory
 * @see Encoder
 * @see Decoder
 */
public class BlockingDirectBinaryEncoder extends DirectBinaryEncoder {
  private final ArrayList<BufferOutputStream> buffers;

  private final ArrayDeque<OutputStream> stashedBuffers;

  private int depth = 0;

  private final ArrayDeque<Long> blockItemCounts;

  /**
   * Create a writer that sends its output to the underlying stream
   * <code>out</code>.
   *
   * @param out The OutputStream to write to
   */
  public BlockingDirectBinaryEncoder(OutputStream out) {
    super(out);
    this.buffers = new ArrayList<>();
    this.stashedBuffers = new ArrayDeque<>();
    this.blockItemCounts = new ArrayDeque<>();
  }

  private void startBlock() {
    stashedBuffers.push(out);
    if (this.buffers.size() <= depth) {
      this.buffers.add(new BufferOutputStream());
    }
    BufferOutputStream buf = buffers.get(depth);
    buf.reset();
    this.depth += 1;
    this.out = buf;
  }

  private void endBlock() {
    if (depth == 0) {
      throw new RuntimeException("Called endBlock, while not buffering a block");
    }
    this.depth -= 1;
    out = stashedBuffers.pop();
    BufferOutputStream buffer = this.buffers.get(depth);
    long blockItemCount = blockItemCounts.pop();
    if (blockItemCount > 0) {
      try {
        // Make it negative, so the reader knows that the number of bytes is coming
        writeLong(-blockItemCount);
        writeLong(buffer.size());
        writeFixed(buffer.toBufferWithoutCopy());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    blockItemCounts.push(itemCount);
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

  private static class BufferOutputStream extends ByteArrayOutputStream {
    BufferOutputStream() {
    }

    ByteBuffer toBufferWithoutCopy() {
      return ByteBuffer.wrap(buf, 0, count);
    }

  }
}
