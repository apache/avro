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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.BinaryDecoder;

/** Read files written by {@link DataFileWriter}.
 * @see DataFileWriter
 */
public class DataFileReader<D> {

  private Schema schema;
  private DatumReader<D> reader;
  private SeekableBufferedInput in;
  private Decoder vin;

  private Map<String,byte[]> meta = new HashMap<String,byte[]>();

  private long count;                           // # entries in file
  private long blockCount;                      // # entries in block
  private byte[] sync = new byte[DataFileWriter.SYNC_SIZE];
  private byte[] syncBuffer = new byte[DataFileWriter.SYNC_SIZE];

  /** Construct a reader for a file. */
  public DataFileReader(SeekableInput sin, DatumReader<D> reader)
    throws IOException {
    this.in = new SeekableBufferedInput(sin);

    byte[] magic = new byte[4];
    in.read(magic);
    if (!Arrays.equals(DataFileWriter.MAGIC, magic))
      throw new IOException("Not a data file.");

    long length = in.length();
    in.seek(length-4);
    int footerSize=(in.read()<<24)+(in.read()<<16)+(in.read()<<8)+in.read();
    in.seek(length-footerSize);
    this.vin = new BinaryDecoder(in);
    long l = vin.readMapStart();
    if (l > 0) {
      do {
        for (long i = 0; i < l; i++) {
          String key = vin.readString(null).toString();
          ByteBuffer value = vin.readBytes(null);
          byte[] bb = new byte[value.remaining()];
          value.get(bb);
          meta.put(key, bb);
        }
      } while ((l = vin.mapNext()) != 0);
    }

    this.sync = getMeta("sync");
    this.count = getMetaLong("count");
    this.schema = Schema.parse(getMetaString("schema"));
    this.reader = reader;

    reader.setSchema(schema);

    in.seek(DataFileWriter.MAGIC.length);         // seek to start
  }

  /** Return the value of a metadata property. */
  public synchronized byte[] getMeta(String key) {
    return meta.get(key);
  }
  /** Return the value of a metadata property. */
  public synchronized String getMetaString(String key) {
    try {
      return new String(getMeta(key), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  /** Return the value of a metadata property. */
  public synchronized long getMetaLong(String key) {
    return Long.parseLong(getMetaString(key));
  }

  /** Return the next datum in the file. */
  public synchronized D next(D reuse) throws IOException {
    while (blockCount == 0) {                     // at start of block

      if (in.tell() == in.length())               // at eof
        return null;

      skipSync();                                 // skip a sync

      blockCount = vin.readLong();                // read blockCount
         
      if (blockCount == DataFileWriter.FOOTER_BLOCK) { 
        in.seek(vin.readLong()+in.tell());        // skip a footer
        blockCount = 0;
      }
    }
    blockCount--;
    return reader.read(reuse, vin);
  }

  private void skipSync() throws IOException {
    vin.readFixed(syncBuffer);
    if (!Arrays.equals(syncBuffer, sync))
      throw new IOException("Invalid sync!");
  }

  /** Move to the specified synchronization point, as returned by {@link
   * DataFileWriter#sync()}. */
  public synchronized void seek(long position) throws IOException {
    in.seek(position);
    blockCount = 0;
  }

  /** Move to the next synchronization point after a position. */
  public synchronized void sync(long position) throws IOException {
    if (in.tell()+DataFileWriter.SYNC_SIZE >= in.length()) {
      in.seek(in.length());
      return;
    }
    in.seek(position);
    vin.readFixed(syncBuffer);
    for (int i = 0; in.tell() < in.length(); i++) {
      int j = 0;
      for (; j < sync.length; j++) {
        if (sync[j] != syncBuffer[(i+j)%sync.length])
          break;
      }
      if (j == sync.length) {                     // position before sync
        in.seek(in.tell() - DataFileWriter.SYNC_SIZE);
        return;
      }
      syncBuffer[i%sync.length] = (byte)in.read();
    }
  }

  /** Close this reader. */
  public synchronized void close() throws IOException {
    in.close();
  }

  private class SeekableBufferedInput extends BufferedInputStream {
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
    public long length() throws IOException { return length; }

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

