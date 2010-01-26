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
package org.apache.avro.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.util.Utf8;

/**
 * Low-level support for de-serializing Avro values.
 *
 *  This class has two types of methods.  One type of methods support
 *  the reading of leaf values (for example, {@link #readLong} and
 *  {@link #readString}).
 *
 *  The other type of methods support the reading of maps and arrays.
 *  These methods are {@link #readArrayStart}, {@link #arrayNext},
 *  and similar methods for maps).  See {@link #readArrayStart} for
 *  details on these methods.)
 *
 *  @see Encoder
 */

public class BinaryDecoder extends Decoder {
  private InputStream in;
  
  private class ByteReader {
    public ByteBuffer read(ByteBuffer old, int length) throws IOException {
      ByteBuffer result;
      if (old != null && length <= old.capacity()) {
        result = old;
        result.clear();
      } else {
        result = ByteBuffer.allocate(length);
      }
      doReadBytes(result.array(), result.position(), length);
      result.limit(length);
      return result;
    }
  }
  
  private class ReuseByteReader extends ByteReader {
    private final ByteBufferInputStream bbi;
    
    public ReuseByteReader(ByteBufferInputStream bbi) {
      this.bbi = bbi;
    }
    
    @Override
    public ByteBuffer read(ByteBuffer old, int length) throws IOException {
      if (old != null) {
        return super.read(old, length);
      } else {
        return bbi.readBuffer(length);
      }
    }
    
  }
  
  private final ByteReader byteReader;

  public BinaryDecoder(InputStream in) {
    this.in = in;
    byteReader = (in instanceof ByteBufferInputStream) ?
        new ReuseByteReader((ByteBufferInputStream) in) : new ByteReader();
  }
  
  @Override
  public void init(InputStream in) {
    this.in = in;
  }

  @Override
  public void readNull() throws IOException { }


  @Override
  public boolean readBoolean() throws IOException {
    int n = in.read();
    if (n < 0) {
      throw new EOFException();
    }
    return n == 1;
  }

  @Override
  public int readInt() throws IOException {
    int n = 0;
    for (int shift = 0; ; shift += 7) {
      int b = in.read();
      if (b >= 0) {
         n |= (b & 0x7F) << shift;
         if ((b & 0x80) == 0) {
           break;
         }
      } else {
        throw new EOFException();
      }
    }
    return (n >>> 1) ^ -(n & 1); // back to two's-complement
  }

  @Override
  public long readLong() throws IOException {
    long n = 0;
    for (int shift = 0; ; shift += 7) {
      long b = in.read();
      if (b >= 0) {
         n |= (b & 0x7F) << shift;
         if ((b & 0x80) == 0) {
           break;
         }
      } else {
        throw new EOFException();
      }
    }
    return (n >>> 1) ^ -(n & 1); // back to two's-complement
  }

  private final byte[] buf = new byte[8];

  @Override
  public float readFloat() throws IOException {
    doReadBytes(buf, 0, 4);
    int n = (((int) buf[0]) & 0xff)
      |  ((((int) buf[1]) & 0xff) << 8)
      |  ((((int) buf[2]) & 0xff) << 16)
      |  ((((int) buf[3]) & 0xff) << 24);
    return Float.intBitsToFloat(n);
  }

  @Override
  public double readDouble() throws IOException {
    doReadBytes(buf, 0, 8);
    long n = (((long) buf[0]) & 0xff)
      |  ((((long) buf[1]) & 0xff) << 8)
      |  ((((long) buf[2]) & 0xff) << 16)
      |  ((((long) buf[3]) & 0xff) << 24)
      |  ((((long) buf[4]) & 0xff) << 32)
      |  ((((long) buf[5]) & 0xff) << 40)
      |  ((((long) buf[6]) & 0xff) << 48)
      |  ((((long) buf[7]) & 0xff) << 56);
    return Double.longBitsToDouble(n);
  }
    
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    int length = readInt();
    Utf8 result = (old != null ? old : new Utf8());
    result.setLength(length);
    doReadBytes(result.getBytes(), 0, length);
    return result;
  }
    
  @Override
  public void skipString() throws IOException {
    doSkipBytes(readInt());
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = readInt();
    return byteReader.read(old, length);
  }

  @Override
  public void skipBytes() throws IOException {
    doSkipBytes(readInt());
  }
  
  @Override
  public void readFixed(byte[] bytes, int start, int length)
    throws IOException {
    doReadBytes(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    doSkipBytes(length);
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }
  
  private void doSkipBytes(long length) throws IOException {
    while (length > 0) {
      long n = in.skip(length);
      if (n <= 0) {
        throw new EOFException();
      }
      length -= n;
    }
  }
  
  /**
   * Reads <tt>length</tt> bytes into <tt>bytes</tt> starting at
   * <tt>start</tt>. 
   * @throws EOFException  If there are not enough number of bytes in
   * the stream.
   * @throws IOException
   */
  private void doReadBytes(byte[] bytes, int start, int length)
    throws IOException {
    for (; ;) {
      int n = in.read(bytes, start, length);
      if (n == length || length == 0) {
        return;
      } else if (n < 0) {
        throw new EOFException();
      }
      start += n;
      length -= n;
    }
  }

  /**
   * Returns the number of items to follow in the current array or map.
   * Returns 0 if there are no more items in the current array and the array/map
   * has ended.
   * @return
   * @throws IOException
   */
  private long doReadItemCount() throws IOException {
    long result = readLong();
    if (result < 0) {
      readLong(); // Consume byte-count if present
      result = -result;
    }
    return result;
  }

  /**
   * Reads the count of items in the current array or map and skip those
   * items, if possible. If it could skip the items, keep repeating until
   * there are no more items left in the array or map. If items cannot be
   * skipped (because byte count to skip is not found in the stream)
   * return the count of the items found. The client needs to skip the
   * items individually.
   * @return  Zero if there are no more items to skip and end of array/map
   * is reached. Positive number if some items are found that cannot be
   * skipped and the client needs to skip them individually.
   * @throws IOException
   */
  private long doSkipItems() throws IOException {
    long result = readInt();
    while (result < 0) {
      long bytecount = readLong();
      doSkipBytes(bytecount);
      result = readInt();
    }
    return result;
  }

  @Override
  public long readArrayStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long arrayNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipArray() throws IOException {
    return doSkipItems();
  }

  @Override
  public long readMapStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long mapNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipMap() throws IOException {
    return doSkipItems();
  }

  @Override
  public int readIndex() throws IOException {
    return readInt();
  }
}

