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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

/** Streaming access to files written by {@link DataFileWriter}.  Use {@link
 * DataFileReader} for file-based input.
 * @see DataFileWriter
 */
public class DataFileStream<D> implements Iterator<D>, Iterable<D> {

  private Schema schema;
  private DatumReader<D> reader;

  /** Raw input stream. */
  final InputStream in;
  /** Decoder on raw input stream.  (Used for metadata.) */
  final Decoder vin;
  /** Secondary decoder, for datums.
   *  (Different than vin for block segments.) */
  Decoder datumIn = null;

  Map<String,byte[]> meta = new HashMap<String,byte[]>();

  long blockRemaining;                          // # entries remaining in block
  byte[] sync = new byte[DataFileConstants.SYNC_SIZE];
  byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];
  private Codec codec;

  /** Construct a reader for an input stream.  For file-based input, use {@link
   * DataFileReader}.  This performs no buffering, for good performance, be
   * sure to pass in a {@link java.io.BufferedInputStream}. */
  public DataFileStream(InputStream in, DatumReader<D> reader)
    throws IOException {
    this.in = in;
    this.vin = new BinaryDecoder(in);

    byte[] magic = new byte[DataFileConstants.MAGIC.length];
    try {
      vin.readFixed(magic);                         // read magic
    } catch (IOException e) {
      throw new IOException("Not a data file.");
    }
    if (!Arrays.equals(DataFileConstants.MAGIC, magic))
      throw new IOException("Not a data file.");

    long l = vin.readMapStart();                  // read meta data
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
    vin.readFixed(sync);                          // read sync

    this.codec = resolveCodec();
    this.schema = Schema.parse(getMetaString(DataFileConstants.SCHEMA));
    this.reader = reader;

    reader.setSchema(schema);
  }

  private Codec resolveCodec() {
    String codecStr = getMetaString(DataFileConstants.CODEC);
    if (codecStr != null) {
      return CodecFactory.fromString(codecStr).createInstance();
    } else {
      return CodecFactory.nullCodec().createInstance();
    }
  }

  /** Return the schema used in this file. */
  public Schema getSchema() { return schema; }

  /** Return the value of a metadata property. */
  public byte[] getMeta(String key) {
    return meta.get(key);
  }
  /** Return the value of a metadata property. */
  public String getMetaString(String key) {
    byte[] value = getMeta(key);
    if (value == null) {
      return null;
    }
    try {
      return new String(value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  /** Return the value of a metadata property. */
  public long getMetaLong(String key) {
    return Long.parseLong(getMetaString(key));
  }

  /** Returns an iterator over entries in this file.  Note that this iterator
   * is shared with other users of the file: it does not contain a separate
   * pointer into the file. */
  public Iterator<D> iterator() { return this; }

  /** True if more entries remain in this file. */
  public boolean hasNext() {
    try {
      if (blockRemaining == 0) {
        // check that the previous block was finished
        // clunky and inefficient with the current Decoder API
        // the only way to detect end of stream is with EOFException
        if (null != datumIn) {
          try {
            datumIn.readBoolean();
            throw new IOException("Block read partially, the data may be corrupt");
          } catch (EOFException eof) {
            // this indicates the block is at its end as we expect
          }
        }
        blockRemaining = vin.readLong();          // read block count
        long compressedSize = vin.readLong();     // read block size
        if (compressedSize > Integer.MAX_VALUE) {
          throw new IOException("Block size too large: " + compressedSize);
        }
        byte[] block = new byte[(int)compressedSize];
        vin.readFixed(block, 0, (int)compressedSize); 
        datumIn = codec.decompress(block);
      }
      return blockRemaining != 0;
    } catch (EOFException e) {                    // at EOF
      return false;
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /** Read the next datum in the file.
   * @throws NoSuchElementException if no more remain in the file.
   */
  public D next() {
    try {
      return next(null);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /** Read the next datum from the file.
   * @param reuse an instance to reuse.
   * @throws NoSuchElementException if no more remain in the file.
   */
  public D next(D reuse) throws IOException {
    if (!hasNext())
      throw new NoSuchElementException();
    D result = reader.read(reuse, datumIn);
    if (--blockRemaining == 0)
      skipSync();
    return result;
  }

  void skipSync() throws IOException {
    vin.readFixed(syncBuffer);
    if (!Arrays.equals(syncBuffer, sync))
      throw new IOException("Invalid sync!");
  }

  /** Not supported. */
  public void remove() { throw new UnsupportedOperationException(); }

  /** Close this reader. */
  public void close() throws IOException {
    in.close();
  }

}

