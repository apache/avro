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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.generic.GenericDatumReader;

/** Stores in a file a sequence of data conforming to a schema.  The schema is
 * stored in the file with the data.  Each datum in a file is of the same
 * schema.  Data is written with a {@link DatumWriter}.  Data is grouped into
 * <i>blocks</i>.  A synchronization marker is written between blocks, so that
 * files may be split.  Blocks may be compressed.  Extensible metadata is
 * stored at the end of the file.  Files may be appended to.
 * @see DataFileReader
 */
public class DataFileWriter<D> {
  private Schema schema;
  private DatumWriter<D> dout;

  private BufferedFileOutputStream out;
  private Encoder vout;

  private Map<String,byte[]> meta = new HashMap<String,byte[]>();

  private long count;                           // # entries in file
  private int blockCount;                       // # entries in current block

  private ByteArrayOutputStream buffer =
    new ByteArrayOutputStream(DataFileConstants.SYNC_INTERVAL*2);
  private Encoder bufOut = new BinaryEncoder(buffer);

  private byte[] sync;                          // 16 random bytes

  /** Construct a writer to a new file for data matching a schema. */
  public DataFileWriter(Schema schema, File file,
                        DatumWriter<D> dout) throws IOException {
    this(schema, new FileOutputStream(file), dout);
  }
  /** Construct a writer to a new file for data matching a schema. */
  public DataFileWriter(Schema schema, OutputStream outs,
                        DatumWriter<D> dout) throws IOException {
    this.schema = schema;
    this.sync = generateSync();

    setMeta(DataFileConstants.SYNC, sync);
    setMeta(DataFileConstants.SCHEMA, schema.toString());
    setMeta(DataFileConstants.CODEC, DataFileConstants.NULL_CODEC);

    init(outs, dout);

    out.write(DataFileConstants.MAGIC);
  }
  
  /** Construct a writer appending to an existing file. */
  public DataFileWriter(File file, DatumWriter<D> dout)
    throws IOException {
    if (!file.exists())
      throw new FileNotFoundException("Not found: "+file);
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    FileDescriptor fd = raf.getFD();
    DataFileReader<D> reader =
      new DataFileReader<D>(new SeekableFileInput(fd),
                            new GenericDatumReader<D>());
    this.schema = reader.getSchema();
    this.sync = reader.sync;
    this.count = reader.getCount();
    this.meta.putAll(reader.meta);

    FileChannel channel = raf.getChannel();       // seek to end
    channel.position(channel.size());

    init(new FileOutputStream(fd), dout);
  }
  
  private void init(OutputStream outs, DatumWriter<D> dout)
    throws IOException {
    this.out = new BufferedFileOutputStream(outs);
    this.vout = new BinaryEncoder(out);
    this.dout = dout;
    dout.setSchema(schema);
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

  /** Set a metadata property. */
  public synchronized void setMeta(String key, byte[] value) {
      meta.put(key, value);
    }
  /** Set a metadata property. */
  public synchronized void setMeta(String key, String value) {
      try {
        setMeta(key, value.getBytes("UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
  /** Set a metadata property. */
  public synchronized void setMeta(String key, long value) {
      setMeta(key, Long.toString(value));
    }

  /** If the schema for this file is a union, add a branch to it. */
  public synchronized void addSchema(Schema branch) {
    if (schema.getType() != Schema.Type.UNION)
      throw new AvroRuntimeException("Not a union schema: "+schema);
    List<Schema> types = schema.getTypes();
    types.add(branch);
    this.schema = Schema.createUnion(types);
    this.dout.setSchema(schema);
    setMeta(DataFileConstants.SCHEMA, schema.toString());
  }

  /** Append a datum to the file. */
  public synchronized void append(D datum) throws IOException {
      dout.write(datum, bufOut);
      blockCount++;
      count++;
      if (buffer.size() >= DataFileConstants.SYNC_INTERVAL)
        writeBlock();
    }

  private void writeBlock() throws IOException {
    if (blockCount > 0) {
      out.write(sync);
      vout.writeLong(blockCount);
      buffer.writeTo(out);
      buffer.reset();
      blockCount = 0;
    }
  }

  /** Return the current position as a value that may be passed to {@link
   * DataFileReader#seek(long)}.  Forces the end of the current block,
   * emitting a synchronization marker. */
  public synchronized long sync() throws IOException {
      writeBlock();
      return out.tell();
    }

  /** Flush the current state of the file, including metadata. */
  public synchronized void flush() throws IOException {
      writeFooter();
      out.flush();
    }

  /** Close the file. */
  public synchronized void close() throws IOException {
      flush();
      out.close();
    }

  private void writeFooter() throws IOException {
    writeBlock();                               // flush any data
    setMeta(DataFileConstants.COUNT, count);    // update count
    bufOut.writeMapStart();              // write meta entries
    bufOut.setItemCount(meta.size());
    for (Map.Entry<String,byte[]> entry : meta.entrySet()) {
      bufOut.startItem();
      bufOut.writeString(entry.getKey());
      bufOut.writeBytes(entry.getValue());
    }
    bufOut.writeMapEnd();
    
    int size = buffer.size()+4;
    out.write(sync);
    vout.writeLong(DataFileConstants.FOOTER_BLOCK);                 // tag the block
    vout.writeLong(size);
    buffer.writeTo(out);
    buffer.reset();
    out.write((byte)(size >>> 24)); out.write((byte)(size >>> 16));
    out.write((byte)(size >>> 8));  out.write((byte)(size >>> 0));
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

