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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.avro.util.Utf8;

/**
 * Low-level support for serializing Avro values.
 *
 * This class has two types of methods.  One type of methods support
 * the writing of leaf values (for example, {@link #writeLong} and
 * {@link #writeString}).  These methods have analogs in {@link
 * ValueReader}.
 *
 * The other type of methods support the writing of maps and arrays.
 * These methods are {@link #writeArrayStart}, {@link
 * #startItem}, and {@link #writeArrayEnd} (and similar methods for
 * maps).  Some implementations of {@link ValueWriter} handle the
 * buffering required to break large maps and arrays into blocks,
 * which is necessary for applications that want to do streaming.
 * (See {@link #writeArrayStart} for details on these methods.)
 *
 *  @see ValueReader
 */
public class ValueWriter {
  protected OutputStream out;
  
  private interface ByteWriter {
    public void write(ByteBuffer bytes) throws IOException;
  }
  
  private static final class SimpleByteWriter implements ByteWriter {
    private final OutputStream out;

    public SimpleByteWriter(OutputStream out) {
      this.out = out;
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      encodeLong(bytes.remaining(), out);
      out.write(bytes.array(), bytes.position(), bytes.remaining());
    }
  }

  private static final class ReuseByteWriter implements ByteWriter {
    private final ByteBufferOutputStream bbout;
    public ReuseByteWriter(ByteBufferOutputStream bbout) {
      this.bbout = bbout;
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      encodeLong(bytes.remaining(), bbout);
      bbout.writeBuffer(bytes);
    }
  }
  
  private final ByteWriter byteWriter;

  /** Create a writer that sends its output to the underlying stream
   *  <code>out</code>. */
  public ValueWriter(OutputStream out) {
    this.out = out;
    this.byteWriter = (out instanceof ByteBufferOutputStream) ?
        new ReuseByteWriter((ByteBufferOutputStream) out) :
          new SimpleByteWriter(out);
  }

  /** Redirect output (and reset the parser state if we're checking). */
  public void init(OutputStream out) throws IOException {
    flush();
    this.out = out;
  }

  /**
   * Writes any buffered output to the underlying stream.
   */
  public void flush() throws IOException {
    out.flush();
  }

  /**
   * "Writes" a null value.  (Doesn't actually write anything, but
   * advances the state of the parser if this class is stateful.)
   * @throws AvroTypeException If this is a stateful writer and a
   *         null is not expected
   */
  public void writeNull() throws IOException { }
  
  /**
   * Write a boolean value.
   * @throws AvroTypeException If this is a stateful writer and a
   * boolean is not expected
   */
  public void writeBoolean(boolean b) throws IOException {
    out.write(b ? 1 : 0);
  }

  /**
   * Writes a 32-bit integer.
   * @throws AvroTypeException If this is a stateful writer and an
   * integer is not expected
   */
  public void writeInt(int n) throws IOException {
    encodeLong(n, out);
  }

  /**
   * Write a 64-bit integer.
   * @throws AvroTypeException If this is a stateful writer and a
   * long is not expected
   */
  public void writeLong(long n) throws IOException {
    encodeLong(n, out);
  }
  
  /** Write a float.
   * @throws IOException 
   * @throws AvroTypeException If this is a stateful writer and a
   * float is not expected
   */
  public void writeFloat(float f) throws IOException {
    encodeFloat(f, out);
  }

  /**
   * Write a double.
   * @throws AvroTypeException If this is a stateful writer and a
   * double is not expected
   */
  public void writeDouble(double d) throws IOException {
    encodeDouble(d, out);
  }

  /**
   * Write a Unicode character string.
   * @throws AvroTypeException If this is a stateful writer and a
   * char-string is not expected
   */
  public void writeString(Utf8 utf8) throws IOException {
    encodeLong(utf8.getLength(), out);
    out.write(utf8.getBytes(), 0, utf8.getLength());
  }

  /**
   * Write a Unicode character string.
   * @throws AvroTypeException If this is a stateful writer and a
   * char-string is not expected
   */
  public void writeString(String str) throws IOException {
    writeString(new Utf8(str));
  }

  /**
   * Write a byte string.
   * @throws AvroTypeException If this is a stateful writer and a
   *         byte-string is not expected
   */
  public void writeBytes(ByteBuffer bytes) throws IOException {
    byteWriter.write(bytes);
  }
  
  /**
   * Write a byte string.
   * @throws AvroTypeException If this is a stateful writer and a
   * byte-string is not expected
   */
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    encodeLong(len, out);
    out.write(bytes, start, len);
  }
  
  /**
   * Writes a byte string.
   * Equivalent to <tt>writeBytes(bytes, 0, bytes.length)</tt>
   * @throws IOException 
   * @throws AvroTypeException If this is a stateful writer and a
   * byte-string is not expected
   */
  public void writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, 0, bytes.length);
  }

  /**
   * Writes a fixed size binary object.
   * @param bytes The contents to write
   * @param start The position within <tt>bytes</tt> where the contents
   * start.
   * @param len The number of bytes to write.
   * @throws AvroTypeException If this is a stateful writer and a
   * byte-string is not expected
   * @throws IOException
   */
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
	  out.write(bytes, start, len);
  }

  /**
   * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
   * @param bytes
   */
  public void writeFixed(byte[] bytes) throws IOException {
    writeFixed(bytes, 0, bytes.length);
  }
  
  /**
   * Writes an enumeration.
   * @param e
   * @throws AvroTypeException If this is a stateful writer and an enumeration
   * is not expected or the <tt>e</tt> is out of range.
   * @throws IOException
   */
  public void writeEnum(int e) throws IOException {
    encodeLong(e, out);
  }

  /** Call this method to start writing an array.
   *
   *  When starting to serialize an array, call {@link
   *  #writeArrayStart}. Then, before writing any data for any item
   *  call {@link #setItemCount} followed by a sequence of
   *  {@link #startItem()} and the item itself. The number of
   *  {@link #startItem()} should match the number specified in
   *  {@link #setItemCount}.
   *  When actually writing the data of the item, you can call any {@link
   *  ValueWriter} method (e.g., {@link #writeLong}).  When all items
   *  of the array have been written, call {@link #writeArrayEnd}.
   *
   *  As an example, let's say you want to write an array of records,
   *  the record consisting of an Long field and a Boolean field.
   *  Your code would look something like this:
   *  <pre>
   *  out.writeArrayStart();
   *  out.setItemCount(list.size());
   *  for (Record r : list) {
   *    out.startItem();
   *    out.writeLong(r.longField);
   *    out.writeBoolean(r.boolField);
   *  }
   *  out.writeArrayEnd();
   *  </pre>
   *  @throws AvroTypeException If this is a stateful writer and an
   *          array is not expected
   */
  public void writeArrayStart() throws IOException {
  }

  /**
   * Call this method before writing a batch of items in an array or a map.
   * Then for each item, call {@link #startItem()} followed by any of the
   * other write methods of {@link ValueWriter}. The number of calls
   * to {@link #startItem()} must be equal to the count specified
   * in {@link #setItemCount}. Once a batch is completed you
   * can start another batch with {@link #setItemCount}.
   * 
   * @param itemCount The number of {@link #startItem()} calls to follow.
   * @throws IOException
   */
  public void setItemCount(long itemCount) throws IOException {
    if (itemCount > 0) {
      writeLong(itemCount);
    }
  }
  
  /**
   * Start a new item of an array or map.
   * See {@link #writeArrayStart} for usage information.
   * @throws AvroTypeException If called outside of an array or map context
   */
  public void startItem() throws IOException {
  }

  /**
   * Call this method to finish writing an array.
   * See {@link #writeArrayStart} for usage information.
   *
   * @throws AvroTypeException If items written does not match count
   *          provided to {@link #writeArrayStart}
   * @throws AvroTypeException If not currently inside an array
   */
  public void writeArrayEnd() throws IOException {
    encodeLong(0, out);
  }

  /**
   * Call this to start a new map.  See
   * {@link #writeArrayStart} for details on usage.
   *
   * As an example of usage, let's say you want to write a map of
   * records, the record consisting of an Long field and a Boolean
   * field.  Your code would look something like this:
   * <pre>
   * out.writeMapStart();
   * out.setItemCount(list.size());
   * for (Map.Entry<String,Record> entry : map.entrySet()) {
   *   out.startItem();
   *   out.writeString(entry.getKey());
   *   out.writeLong(entry.getValue().longField);
   *   out.writeBoolean(entry.getValue().boolField);
   * }
   * out.writeMapEnd();
   * </pre>
   * @throws AvroTypeException If this is a stateful writer and a
   * map is not expected
   */
  public void writeMapStart() throws IOException {
  }

  /**
   * Call this method to terminate the inner-most, currently-opened
   * map.  See {@link #writeArrayStart} for more details.
   *
   * @throws AvroTypeException If items written does not match count
   *          provided to {@link #writeMapStart}
   * @throws AvroTypeException If not currently inside a map
   */
  public void writeMapEnd() throws IOException {
    encodeLong(0, out);
  }

  /** Call this method to write the tag of a union.
   *
   * As an example of usage, let's say you want to write a union,
   * whose second branch is a record consisting of an Long field and
   * a Boolean field.  Your code would look something like this:
   * <pre>
   * out.writeIndex(1);
   * out.writeLong(record.longField);
   * out.writeBoolean(record.boolField);
   * </pre>
   * @throws AvroTypeException If this is a stateful writer and a
   * map is not expected
   */
  public void writeIndex(int unionIndex) throws IOException {
    encodeLong(unionIndex, out);
  }
  
  protected static void encodeLong(long n, OutputStream o) throws IOException {
    n = (n << 1) ^ (n >> 63); // move sign to low-order bit
    while ((n & ~0x7F) != 0) {
      o.write((byte)((n & 0x7f) | 0x80));
      n >>>= 7;
    }
    o.write((byte)n);
  }

  protected static int encodeLong(long n, byte[] b, int pos) {
    n = (n << 1) ^ (n >> 63); // move sign to low-order bit
    while ((n & ~0x7F) != 0) {
      b[pos++] = (byte)((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    b[pos++] = (byte) n;
    return pos;
  }

  protected static void encodeFloat(float f, OutputStream o) throws IOException {
    long bits = Float.floatToRawIntBits(f);
    o.write((int)(bits      ) & 0xFF);
    o.write((int)(bits >>  8) & 0xFF);
    o.write((int)(bits >> 16) & 0xFF);
    o.write((int)(bits >> 24) & 0xFF);
  }

  protected static int encodeFloat(float f, byte[] b, int pos) {
    long bits = Float.floatToRawIntBits(f);
    b[pos++] = (byte)((bits      ) & 0xFF);
    b[pos++] = (byte)((bits >>  8) & 0xFF);
    b[pos++] = (byte)((bits >> 16) & 0xFF);
    b[pos++] = (byte)((bits >> 24) & 0xFF);
    return pos;
  }

  protected static void encodeDouble(double d, OutputStream o) throws IOException {
    long bits = Double.doubleToRawLongBits(d);
    o.write((int)(bits      ) & 0xFF);
    o.write((int)(bits >>  8) & 0xFF);
    o.write((int)(bits >> 16) & 0xFF);
    o.write((int)(bits >> 24) & 0xFF);
    o.write((int)(bits >> 32) & 0xFF);
    o.write((int)(bits >> 40) & 0xFF);
    o.write((int)(bits >> 48) & 0xFF);
    o.write((int)(bits >> 56) & 0xFF);
  }

  protected static int encodeDouble(double d, byte[] b, int pos) {
    long bits = Double.doubleToRawLongBits(d);
    b[pos++] = (byte)((bits      ) & 0xFF);
    b[pos++] = (byte)((bits >>  8) & 0xFF);
    b[pos++] = (byte)((bits >> 16) & 0xFF);
    b[pos++] = (byte)((bits >> 24) & 0xFF);
    b[pos++] = (byte)((bits >> 32) & 0xFF);
    b[pos++] = (byte)((bits >> 40) & 0xFF);
    b[pos++] = (byte)((bits >> 48) & 0xFF);
    b[pos++] = (byte)((bits >> 56) & 0xFF);
    return pos;
  }
}
