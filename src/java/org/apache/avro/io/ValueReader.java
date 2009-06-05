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

import org.apache.avro.AvroTypeException;
import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.util.Utf8;

/**
 * Low-level support for de-serializing Avro values.
 *
 *  This class has two types of methods.  One type of methods support
 *  the reading of leaf values (for example, {@link #readLong} and
 *  {@link #readString}).
 *
 *  The other type of methods support the writing of maps and arrays.
 *  These methods are {@link #readArrayStart}, {@link #arrayNext},
 *  and similar methods for maps).  See {@link #readArrayStart} for
 *  details on these methods.)
 *
 *  @see ValueWriter
 */

public class ValueReader {
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

  public ValueReader(InputStream in) {
    this.in = in;
    byteReader = (in instanceof ByteBufferInputStream) ?
        new ReuseByteReader((ByteBufferInputStream) in) : new ByteReader();
  }
  
  /** Start reading against a different input stream.  Stateful
    * subclasses will reset their states to their initial state. */
  public void init(InputStream in) {
    this.in = in;
  }

  /**
   * "Reads" a null value.  (Doesn't actually read anything, but
   * advances the state of the parser if the implementation is
   * stateful.)
   *  @throws AvroTypeException If this is a stateful reader and
   *          null is not the type of the next value to be read
   */
  public void readNull() throws IOException { }

  /**
   * Reads a boolean value written by {@link ValueWriter#writeBoolean}.
   * @throws AvroTypeException If this is a stateful reader and
   * boolean is not the type of the next value to be read
   */

  public boolean readBoolean() throws IOException {
    int n = in.read();
    if (n < 0) {
      throw new EOFException();
    }
    return n == 1;
  }

  /**
   * Reads an integer written by {@link ValueWriter#writeInt}.
   * @throws AvroTypeException If encoded value is larger than
   *          32-bits
   * @throws AvroTypeException If this is a stateful reader and
   *          int is not the type of the next value to be read
   */
  public int readInt() throws IOException {
    long result = readLong();
    if (result < Integer.MIN_VALUE || Integer.MAX_VALUE < result) {
      throw new AvroTypeException("Integer overflow.");
    }
    return (int)result;
  }

  /**
   * Reads a long written by {@link ValueWriter#writeLong}.
   * @throws AvroTypeException If this is a stateful reader and
   *          long is not the type of the next value to be read
   */
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

  /**
   * Reads a float written by {@link ValueWriter#writeFloat}.
   * @throws AvroTypeException If this is a stateful reader and
   * is not the type of the next value to be read
   */
  public float readFloat() throws IOException {
    int n = 0;
    for (int i = 0, shift = 0; i < 4; i++, shift += 8) {
      int k = in.read();
      if (k >= 0) {
        n |= (k & 0xff) << shift;
      } else {
        throw new EOFException();
      }
    }
    return Float.intBitsToFloat(n);
  }

  /**
   * Reads a double written by {@link ValueWriter#writeDouble}.
   * @throws AvroTypeException If this is a stateful reader and
   *           is not the type of the next value to be read
   */
  public double readDouble() throws IOException {
    long n = 0;
    for (int i = 0, shift = 0; i < 8; i++, shift += 8) {
      long k = in.read();
      if (k >= 0) {
        n |= (k & 0xff) << shift;
      } else {
        throw new EOFException();
      }
    }
    return Double.longBitsToDouble(n);
  }
    
  /**
   * Reads a char-string written by {@link ValueWriter#writeString}.
   * @throws AvroTypeException If this is a stateful reader and
   * char-string is not the type of the next value to be read
   */
  public Utf8 readString(Utf8 old) throws IOException {
    int length = readInt();
    Utf8 result = (old != null ? old : new Utf8());
    result.setLength(length);
    doReadBytes(result.getBytes(), 0, length);
    return result;
  }
    
  /**
   * Discards a char-string written by {@link ValueWriter#writeString}.
   *  @throws AvroTypeException If this is a stateful reader and
   *          char-string is not the type of the next value to be read
   */
  public void skipString() throws IOException {
    doSkipBytes(readInt());
  }

  /**
   * Reads a byte-string written by {@link ValueWriter#writeBytes}.
   * if <tt>old</tt> is not null and has sufficient capacity to take in
   * the bytes being read, the bytes are returned in <tt>old</tt>.
   * @throws AvroTypeException If this is a stateful reader and
   *          byte-string is not the type of the next value to be read
   */
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = readInt();
    return byteReader.read(old, length);
  }

  /**
   * Discards a byte-string written by {@link ValueWriter#writeBytes}.
   *  @throws AvroTypeException If this is a stateful reader and
   *          byte-string is not the type of the next value to be read
   */
  public void skipBytes() throws IOException {
    doSkipBytes(readInt());
  }
  
  /**
   * Reads fixed sized binary object.
   * @param bytes The buffer to store the contents being read.
   * @param start The position where the data needs to be written.
   * @param length  The size of the binary object.
   * @throws AvroTypeException If this is a stateful reader and
   *          fixed sized binary object is not the type of the next
   *          value to be read or the length is incorrect.
   * @throws IOException
   */
  public void readFixed(byte[] bytes, int start, int length)
    throws IOException {
    doReadBytes(bytes, start, length);
  }

  /**
   * A shorthand for <tt>readFixed(bytes, 0, bytes.length)</tt>.
   * @throws AvroTypeException If this is a stateful reader and
   *          fixed sized binary object is not the type of the next
   *          value to be read or the length is incorrect.
   * @throws IOException
   */
  public void readFixed(byte[] bytes) throws IOException {
    readFixed(bytes, 0, bytes.length);
  }
  
  /**
   * Discards fixed sized binary object.
   * @param length  The size of the binary object to be skipped.
   * @throws AvroTypeException If this is a stateful reader and
   *          fixed sized binary object is not the type of the next
   *          value to be read or the length is incorrect.
   * @throws IOException
   */
  public void skipFixed(int length) throws IOException {
    doSkipBytes(length);
  }

  /**
   * Reads an enumeration.
   * @return The enumeration's value.
   * @throws AvroTypeException If this is a stateful reader and
   *          enumeration is not the type of the next value to be read.
   * @throws IOException
   */
  public int readEnum() throws IOException {
    return readInt();
  }
  
  private void doSkipBytes(long length) throws IOException, EOFException {
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
    while (length > 0) {
      int n = in.read(bytes, start, length);
      if (n < 0) throw new EOFException();
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

  /**
   * Reads and returns the size of the first block of an array.  If
   * this method returns non-zero, then the caller should read the
   * indicated number of items, and then call {@link
   * #arrayNext} to find out the number of items in the next
   * block.  The typical pattern for consuming an array looks like:
   * <pre>
   *   for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
   *     for (long j = 0; j < i; j++) {
   *       read next element of the array;
   *     }
   *   }
   * </pre>
   *  @throws AvroTypeException If this is a stateful reader and
   *          array is not the type of the next value to be read */
  public long readArrayStart() throws IOException {
    return doReadItemCount();
  }

  /**
   * Processes the next block of an array andreturns the number of items in
   * the block and let's the caller
   * read those items.
   * @throws AvroTypeException When called outside of an
   *         array context
   */
  public long arrayNext() throws IOException {
    return doReadItemCount();
  }

  /**
   * Used for quickly skipping through an array.  Note you can
   * either skip the entire array, or read the entire array (with
   * {@link #readArrayStart}), but you can't mix the two on the
   * same array.
   *
   * This method will skip through as many items as it can, all of
   * them if possible.  It will return zero if there are no more
   * items to skip through, or an item count if it needs the client's
   * help in skipping.  The typical usage pattern is:
   * <pre>
   *   for(long i = in.skipArray(); i != 0; i = i.skipArray()) {
   *     for (long j = 0; j < i; j++) {
   *       read and discard the next element of the array;
   *     }
   *   }
   * </pre>
   * Note that this method can automatically skip through items if a
   * byte-count is found in the underlying data, or if a schema has
   * been provided to the implementation, but
   * otherwise the client will have to skip through items itself.
   *
   *  @throws AvroTypeException If this is a stateful reader and
   *          array is not the type of the next value to be read
   */
  public long skipArray() throws IOException {
    return doSkipItems();
  }

  /**
   * Reads and returns the size of the next block of map-entries.
   * Similar to {@link #readArrayStart}.
   *
   *  As an example, let's say you want to read a map of records,
   *  the record consisting of an Long field and a Boolean field.
   *  Your code would look something like this:
   * <pre>
   *   Map<String,Record> m = new HashMap<String,Record>();
   *   Record reuse = new Record();
   *   for(long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
   *     for (long j = 0; j < i; j++) {
   *       String key = in.readString();
   *       reuse.intField = in.readInt();
   *       reuse.boolField = in.readBoolean();
   *       m.put(key, reuse);
   *     }
   *   }
   * </pre>
   * @throws AvroTypeException If this is a stateful reader and
   *         map is not the type of the next value to be read
   */
  public long readMapStart() throws IOException {
    return doReadItemCount();
  }

  /**
   * Processes the next block of map entries and returns the count of them.
   * Similar to {@link #arrayNext}.  See {@link #readMapStart} for details.
   * @throws AvroTypeException When called outside of a
   *         map context
   */
  public long mapNext() throws IOException {
    return doReadItemCount();
  }

  /**
   * Support for quickly skipping through a map similar to {@link #skipArray}.
   *
   * As an example, let's say you want to skip a map of records,
   * the record consisting of an Long field and a Boolean field.
   * Your code would look something like this:
   * <pre>
   *   for(long i = in.skipMap(); i != 0; i = in.skipMap()) {
   *     for (long j = 0; j < i; j++) {
   *       in.skipString();  // Discard key
   *       in.readInt(); // Discard int-field of value
   *       in.readBoolean(); // Discard boolean-field of value
   *     }
   *   }
   * </pre>
   *  @throws AvroTypeException If this is a stateful reader and
   *          array is not the type of the next value to be read */

  public long skipMap() throws IOException {
    return doSkipItems();
  }

  /**
   * Reads the tag of a union written by {@link ValueWriter#writeIndex}.
   * @throws AvroTypeException If this is a stateful reader and
   *         union is not the type of the next value to be read
   */
  public int readIndex() throws IOException {
    return readInt();
  }
}
