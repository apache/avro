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

import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.File;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;

/** Random access to files written with {@link DataFileWriter}.
 * @see DataFileWriter
 */
public class DataFileReader<D> extends DataFileStream<D> {
  private SeekableInputStream sin;
  private long blockStart;

  /** Construct a reader for a file. */
  public DataFileReader(File file, DatumReader<D> reader) throws IOException {
    this(new SeekableFileInput(file), reader);
  }

  /** Construct a reader for a file. */
  public DataFileReader(SeekableInput sin, DatumReader<D> reader)
    throws IOException {
    super(reader);
    this.sin = new SeekableInputStream(sin);
    initialize(this.sin);
    blockFinished();
  }

  /** Move to a specific, known synchronization point, one returned from {@link
   * DataFileWriter#sync()} while writing.  If synchronization points were not
   * saved while writing a file, use {#sync(long)} instead. */
  public void seek(long position) throws IOException {
    sin.seek(position);
    vin = DecoderFactory.defaultFactory().createBinaryDecoder(this.sin, vin);
    datumIn = null;
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
      int i=0, b;
      InputStream in = vin.inputStream();
      vin.readFixed(syncBuffer);
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
    } catch (EOFException e) {
      // fall through
    }
    // if no match or EOF set start to the end position
      blockStart = sin.tell();
    //System.out.println("block start location after EOF: " + blockStart );
      return;
  }

  @Override
  protected void blockFinished() throws IOException {
    blockStart = sin.tell() - vin.inputStream().available();
  }

  /** Return the last synchronization point before our current position. */
  public long previousSync() {
    return blockStart;
  }

  /** Return true if past the next synchronization point after a position. */ 
  public boolean pastSync(long position) throws IOException {
    return ((blockStart >= position+SYNC_SIZE)||(blockStart >= sin.length()));
  }

  private static class SeekableInputStream extends InputStream 
  implements SeekableInput {
    private final byte[] oneByte = new byte[1];
    private SeekableInput in;

    SeekableInputStream(SeekableInput in) throws IOException {
        this.in = in;
      }
    
    @Override
    public void seek(long p) throws IOException {
      if (p < 0)
        throw new IOException("Illegal seek: " + p);
      in.seek(p);
      }

    @Override
    public long tell() throws IOException {
      return in.tell();
    }

    @Override
    public long length() throws IOException {
      return in.length();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return in.read(b, 0, b.length);
      }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      int n = read(oneByte, 0, 1);
      if (n == 1) {
        return oneByte[0] & 0xff;
      } else {
        return n;
    }
    };

    @Override
    public long skip(long skip) throws IOException {
      long position = in.tell();
      long length = in.length();
      long remaining = length - position;
      if (remaining > skip) {
        in.seek(skip);
        return in.tell() - position;
      } else {
        in.seek(remaining);
        return in.tell() - position;
    }
  }

    @Override
    public int available() throws IOException {
      long remaining = (in.length() - in.tell());
      return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE
          : (int) remaining;
    }
  }
}

