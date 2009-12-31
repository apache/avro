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
package org.apache.avro.file;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.File;

import org.apache.avro.io.DatumReader;
import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;

/** Random access to files written with {@link DataFileWriter}.
 * @see DataFileWriter
 */
public class DataFileReader<D> extends DataFileStream<D> {
  private SeekableBufferedInput sin;
  private long blockStart;

  /** Construct a reader for a file. */
  public DataFileReader(File file, DatumReader<D> reader) throws IOException {
    this(new SeekableFileInput(file), reader);
  }

  /** Construct a reader for a file. */
  public DataFileReader(SeekableInput sin, DatumReader<D> reader)
    throws IOException {
    super(new SeekableBufferedInput(sin), reader);
    this.sin = (SeekableBufferedInput)in;
  }

  /** Move to a specific, known synchronization point, one returned from {@link
   * DataFileWriter#sync()} while writing.  If synchronization points were not
   * saved while writing a file, use {#sync(long)} instead. */
  public void seek(long position) throws IOException {
    sin.seek(position);
    blockRemaining = 0;
    blockStart = position;
  }

  /** Move to the next synchronization point after a position. To process a
   * range of file entires, call this with the starting position, then check
   * {@link #pastSync(long)} with the end point before each call to {@link
   * #next()}. */
  public void sync(long position) throws IOException {
    seek(position);
    try {
      vin.readFixed(syncBuffer);
    } catch (EOFException e) {
      blockStart = sin.tell();
      return;
    }
    int i=0, b;
    do {
      int j = 0;
      for (; j < SYNC_SIZE; j++) {
        if (sync[j] != syncBuffer[(i+j)%SYNC_SIZE])
          break;
      }
      if (j == SYNC_SIZE) {                       // matched a complete sync
        blockStart = position + i + SYNC_SIZE;
        return;
      }
      b = in.read();
      syncBuffer[i++%SYNC_SIZE] = (byte)b;
    } while (b != -1);
  }

  @Override
  void skipSync() throws IOException {            // note block start
    super.skipSync();
    blockStart = sin.tell();
  }

  /** Return true if past the next synchronization point after a position. */ 
  public boolean pastSync(long position) {
    return blockStart >= Math.min(sin.length(), position+SYNC_SIZE);
  }

  private static class SeekableBufferedInput extends BufferedInputStream {
    private long position;                        // end of buffer
    private long length;                          // file length

    private class PositionFilter extends InputStream {
      private SeekableInput in;
      public PositionFilter(SeekableInput in) throws IOException {
        this.in = in;
      }
      public int read() { throw new UnsupportedOperationException(); }
      public int read(byte[] b, int off, int len) throws IOException {
        int value = in.read(b, off, len);
        if (value > 0) position += value;         // update on read
        return value;
      }
    }

    public SeekableBufferedInput(SeekableInput in) throws IOException {
      super(null);
      this.in = new PositionFilter(in);
      this.length = in.length();
    }

    public void seek(long p) throws IOException {
      if (p < 0) throw new IOException("Illegal seek: "+p);
      long start = position - count;
      if (p >= start && p < position) {            // in buffer
        this.pos = (int)(p - start);
      } else {                                     // not in buffer
        this.pos = 0;
        this.count = 0;
        ((PositionFilter)in).in.seek(p);
        this.position = p;
      }
    }

    public long tell() { return position-(count-pos); }
    public long length() { return length; }

    public int read() throws IOException {        // optimized implementation
      if (pos >= count) return super.read();
      return buf[pos++] & 0xff;
    }

    public long skip(long skip) throws IOException { // optimized implementation
      if (skip > count-pos)
        return super.skip(skip);
      pos += (int)skip;
      return skip;
    }
  }

}

