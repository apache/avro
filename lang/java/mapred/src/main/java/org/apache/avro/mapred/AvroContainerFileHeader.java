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

package org.apache.avro.mapred;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Keys for hadoop files opened with AvroEncodedInputFormat.
 *
 * <p>The data wrapped in this object corresponds to a <i>file header</i> in an Avro container
 * file according to the
 * <a href="http://avro.apache.org/docs/current/spec.html#Object+Container+Files">spec</a>.</p>
 */
public class AvroContainerFileHeader implements WritableComparable<AvroContainerFileHeader> {

  static final byte[] SYNC_MARKER = new byte[DataFileConstants.SYNC_SIZE];
  static private final int SCHEMA_LENGTH_VARINT_BYTES_MAX = 5;
  static private final byte[] HEAD;
  static private final byte[] TAIL;

  static {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
    try {
      encoder.writeFixed(DataFileConstants.MAGIC);
      encoder.writeMapStart();
      encoder.setItemCount(1L);
      encoder.startItem();
      encoder.writeString(DataFileConstants.SCHEMA);
      HEAD = stream.toByteArray();
      stream.reset();
      encoder.writeMapEnd();
      encoder.writeFixed(SYNC_MARKER);
      TAIL = stream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BytesWritable header;
  private int schemaOffset;
  private int schemaLength;

  /**
   * No-arg constructor.
   *
   * Instance remains invalid until writer schema is set using
   * {@link AvroContainerFileHeader#set(Schema)} or until
   * {@link AvroContainerFileHeader#readFields(DataInput)} is called.
   */
  public AvroContainerFileHeader() {
    header = new BytesWritable(new byte[DataFileConstants.DEFAULT_SYNC_INTERVAL]);
    header.set(HEAD, 0, HEAD.length);
  }

  /**
   * Wrap a writer schema.
   */
  public AvroContainerFileHeader(Schema writerSchema) {
    this();
    set(writerSchema);
  }

  /**
   * Schema for Avro container file headers, as defined in the spec.
   */
  public static Schema fileHeaderSchema() {
    return SchemaBuilder
      .record("org.apache.avro.file.Header")
      .fields()
      .name("magic")
      .type(SchemaBuilder.fixed("Magic").size(DataFileConstants.MAGIC.length))
      .noDefault()
      .name("meta")
      .type(SchemaBuilder.map().values(Schema.create(Schema.Type.BYTES)))
      .noDefault()
      .name("sync")
      .type(SchemaBuilder.fixed("Magic").size(DataFileConstants.SYNC_SIZE))
      .noDefault()
      .endRecord();
  }

  /**
   * Sets writer schema for this instance.
   */
  public void set(Schema writerSchema) {
    final byte[] json;
    try {
      json = writerSchema.toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    resetBuffer(json.length);
    System.arraycopy(json, 0, header.getBytes(), schemaOffset, schemaLength);
    finalizeBuffer();
  }

  /**
   * Returns copy of writer schema.
   */
  public Schema getWriterSchema() {
    InputStream stream = new ByteArrayInputStream(header.getBytes(), schemaOffset, schemaLength);
    try {
      return new Schema.Parser().parse(stream);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Exposes the underlying BytesWritable instance containing the encoded file header object.
   */
  public BytesWritable unwrap() {
    return header;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(AvroContainerFileHeader o) {
    return header.compareTo(o.header);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(schemaLength);
    out.write(header.getBytes(), schemaOffset, schemaLength);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    resetBuffer(in.readInt());
    in.readFully(header.getBytes(), schemaOffset, schemaLength);
    finalizeBuffer();
  }

  private void resetBuffer(int newSchemaLength) {
    schemaLength = newSchemaLength;
    header.setSize(HEAD.length + SCHEMA_LENGTH_VARINT_BYTES_MAX + schemaLength + TAIL.length);
    schemaOffset = HEAD.length;
    schemaOffset += BinaryData.encodeInt(schemaLength, header.getBytes(), schemaOffset);
    header.setSize(schemaOffset + schemaLength + TAIL.length);
  }

  private void finalizeBuffer() {
    System.arraycopy(TAIL, 0, header.getBytes(), schemaOffset + schemaLength, TAIL.length);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof AvroContainerFileHeader)) return false;
    AvroContainerFileHeader that = (AvroContainerFileHeader) o;
    return header.equals(that.header);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return header.hashCode();
  }
}
