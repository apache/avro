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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

/** Stores in a file a sequence of data conforming to a schema.  The schema is
 * stored in the file with the data.  Each datum in a file is of the same
 * schema.  Data is written with a {@link DatumWriter}.  Data is grouped into
 * <i>blocks</i>.  A synchronization marker is written between blocks, so that
 * files may be split.  Blocks may be compressed.  Extensible metadata is
 * stored at the end of the file.  Files may be appended to.
 * @see DataFileReader
 */
public class DataFileWriter<D> implements Closeable, Flushable {
  private Schema schema;
  private DatumWriter<D> dout;

  private BufferedFileOutputStream out;
  private Encoder vout;

  private final Map<String,byte[]> meta = new HashMap<String,byte[]>();

  private int blockCount;                       // # entries in current block

  private ByteArrayOutputStream buffer;
  private Encoder bufOut;

  private byte[] sync;                          // 16 random bytes
  private int syncInterval = DataFileConstants.DEFAULT_SYNC_INTERVAL;

  private boolean isOpen;
  private Codec codec;

  /** Construct a writer, not yet open. */
  public DataFileWriter(DatumWriter<D> dout) {
    this.dout = dout;
  }
  
  private void assertOpen() {
    if (!isOpen) throw new AvroRuntimeException("not open");
  }
  private void assertNotOpen() {
    if (isOpen) throw new AvroRuntimeException("already open");
  }
  
  /** 
   * Configures this writer to use the given codec. 
   * May not be reset after writes have begun.
   */
  public DataFileWriter<D> setCodec(CodecFactory c) {
    assertNotOpen();
    this.codec = c.createInstance();
    setMetaInternal(DataFileConstants.CODEC, codec.getName());
    return this;
  }

  /** Set the synchronization interval for this file, in bytes. */
  public DataFileWriter<D> setSyncInterval(int syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  /** Open a new file for data matching a schema. */
  public DataFileWriter<D> create(Schema schema, File file) throws IOException {
    return create(schema, new FileOutputStream(file));
  }

  /** Open a new file for data matching a schema. */
  public DataFileWriter<D> create(Schema schema, OutputStream outs)
    throws IOException {
    assertNotOpen();

    this.schema = schema;
    setMetaInternal(DataFileConstants.SCHEMA, schema.toString());
    this.sync = generateSync();

    init(outs);

    out.write(DataFileConstants.MAGIC);           // write magic

    vout.writeMapStart();                         // write metadata
    vout.setItemCount(meta.size());
    for (Map.Entry<String,byte[]> entry : meta.entrySet()) {
      vout.startItem();
      vout.writeString(entry.getKey());
      vout.writeBytes(entry.getValue());
    }
    vout.writeMapEnd();

    out.write(sync);                              // write initial sync

    return this;
  }

  /** Open a writer appending to an existing file. */
  public DataFileWriter<D> appendTo(File file) throws IOException {
    assertNotOpen();
    if (!file.exists())
      throw new FileNotFoundException("Not found: "+file);
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    FileDescriptor fd = raf.getFD();
    DataFileReader<D> reader =
      new DataFileReader<D>(new SeekableFileInput(fd),
                            new GenericDatumReader<D>());
    this.schema = reader.getSchema();
    this.sync = reader.sync;
    this.meta.putAll(reader.meta);
    byte[] codecBytes = this.meta.get(DataFileConstants.CODEC);
    if (codecBytes != null) {
      String strCodec = new String(codecBytes, "UTF-8");
      this.codec = CodecFactory.fromString(strCodec).createInstance();
    } else {
      this.codec = CodecFactory.nullCodec().createInstance();
    }

    FileChannel channel = raf.getChannel();       // seek to end
    channel.position(channel.size());

    init(new FileOutputStream(fd));

    return this;
  }

  private void init(OutputStream outs) throws IOException {
    this.out = new BufferedFileOutputStream(outs);
    this.vout = new BinaryEncoder(out);
    dout.setSchema(schema);
    this.buffer = new ByteArrayOutputStream(syncInterval*2);
    if (this.codec == null) {
      this.codec = CodecFactory.nullCodec().createInstance();
    }
    this.bufOut = new BinaryEncoder(buffer);
    this.isOpen = true;
  }

  private static byte[] generateSync() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((new UID()+"@"+time).getBytes());
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private DataFileWriter<D> setMetaInternal(String key, byte[] value) {
    assertNotOpen();
    meta.put(key, value);
    return this;
  }
  
  public DataFileWriter<D> setMetaInternal(String key, String value) {
    try {
      return setMetaInternal(key, value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, byte[] value) {
    if (isReserved(key)) {
      throw new AvroRuntimeException("Cannot set reserved meta key: " + key);
    }
    return setMetaInternal(key, value);
  }
  
  private boolean isReserved(String key) {
    return key.startsWith("avro.");
  }

  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, String value) {
    try {
      return setMeta(key, value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, long value) {
    return setMeta(key, Long.toString(value));
  }

  /** Append a datum to the file. */
  public void append(D datum) throws IOException {
    assertOpen();
    dout.write(datum, bufOut);
    blockCount++;
    if (buffer.size() >= syncInterval)
      writeBlock();
  }

  private void writeBlock() throws IOException {
    if (blockCount > 0) {
      vout.writeLong(blockCount);
      codec.compress(buffer, out);
      buffer.reset();
      blockCount = 0;
      out.write(sync);
    }
  }

  /** Return the current position as a value that may be passed to {@link
   * DataFileReader#seek(long)}.  Forces the end of the current block,
   * emitting a synchronization marker. */
  public long sync() throws IOException {
    assertOpen();
    writeBlock();
    return out.tell();
  }

  /** Flush the current state of the file. */
  public void flush() throws IOException {
    sync();
    out.flush();
  }

  /** Close the file. */
  public void close() throws IOException {
    flush();
    out.close();
    isOpen = false;
  }

  private class BufferedFileOutputStream extends BufferedOutputStream {
    private long position;                         // start of buffer

    private class PositionFilter extends FilterOutputStream {
      public PositionFilter(OutputStream out) throws IOException { super(out); }
      public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        position += len;                           // update on write
      }
    }

    public BufferedFileOutputStream(OutputStream out) throws IOException {
      super(null);
      this.out = new PositionFilter(out);
    }

    public long tell() { return position+count; }
  }

}

