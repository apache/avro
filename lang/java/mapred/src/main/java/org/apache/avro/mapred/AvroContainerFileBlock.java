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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryData;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Values for hadoop files opened with AvroEncodedInputFormat.
 *
 * <p>The data wrapped in this object corresponds to a <i>file data block</i> in an Avro container
 * file according to the
 * <a href="http://avro.apache.org/docs/current/spec.html#Object+Container+Files">spec</a>.</p>
 */
public class AvroContainerFileBlock implements WritableComparable<AvroContainerFileBlock> {

  static private final int BLOCK_OVERHEAD_BYTES_MAX;

  static {
    final int varLongBytesMax = 9;
    final int varIntBytesMax = 5;
    BLOCK_OVERHEAD_BYTES_MAX = varLongBytesMax + varIntBytesMax +
      AvroContainerFileHeader.SYNC_MARKER.length;
  }

  private BytesWritable block;
  private int objectsOffset;
  private int objectsLength;
  private long objectCount;

  /**
   * No-arg constructor for empty block.
   */
  public AvroContainerFileBlock() {
    this(0L, new byte[0], 0, 0);
  }

  /**
   * Wraps an uncompressed, binary-encoded sequence of objects.
   */
  public AvroContainerFileBlock(long objectCount, byte[] encodedObjects) {
    this(objectCount, encodedObjects, 0, encodedObjects.length);
  }

  /**
   * Wraps an uncompressed, binary-encoded sequence of objects, in range [pos, pos+len[.
   */
  public AvroContainerFileBlock(long objectCount, byte[] encodedObjects, int pos, int len) {
    block = new BytesWritable(new byte[DataFileConstants.DEFAULT_SYNC_INTERVAL]);
    set(objectCount, encodedObjects, pos, len);
  }

  /**
   * Schema for Avro container file blocks, as defined in the spec.
   */
  public static Schema fileBlockSchema() {
    return SchemaBuilder
      .record("org.apache.avro.file.Block")
      .fields()
      .name("objects")
      .type(Schema.create(Schema.Type.LONG))
      .noDefault()
      .name("bytes")
      .type(Schema.create(Schema.Type.BYTES))
      .noDefault()
      .name("sync")
      .type(SchemaBuilder.fixed("Magic").size(DataFileConstants.SYNC_SIZE))
      .noDefault()
      .endRecord();
  }

  /**
   * Wraps an uncompressed, binary-encoded sequence of objects.
   */
  public void set(long objectCount, byte[] encodedObjects) {
    set(objectCount, encodedObjects, 0, encodedObjects.length);
  }

  /**
   * Wraps an uncompressed, binary-encoded sequence of objects, in range [pos, pos+len[.
   */
  public void set(long objectCount, byte[] encodedObjects, int pos, int len) {
    resetBuffer(objectCount, len);
    System.arraycopy(encodedObjects, pos, block.getBytes(), objectsOffset, objectsLength);
    finalizeBuffer();
  }

  /**
   * Generates a ByteBuffer wrapping the encoded objects.
   */
  public ByteBuffer getEncodedObjects() {
    return ByteBuffer.wrap(block.getBytes(), objectsOffset, objectsLength);
  }

  /**
   * Generates an InputStream on the encoded objects.
   */
  public InputStream getEncodedObjectStream() {
    return new ByteArrayInputStream(block.getBytes(), objectsOffset, objectsLength);
  }

  /**
   * Returns the object count in this block.
   */
  public long getObjectCount() {
    return objectCount;
  }

  /**
   * Exposes the underlying BytesWritable instance containing the encoded file block object.
   */
  public BytesWritable unwrap() {
    return block;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(AvroContainerFileBlock o) {
    return block.compareTo(o.block);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(objectCount);
    out.writeInt(objectsLength);
    out.write(block.getBytes(), objectsOffset, objectsLength);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    resetBuffer(in.readLong(), in.readInt());
    in.readFully(block.getBytes(), objectsOffset, objectsLength);
    finalizeBuffer();
  }

  private void resetBuffer(long newBlockCount, int newBlockLength) {
    objectCount = newBlockCount;
    objectsLength = newBlockLength;
    block.setSize(BLOCK_OVERHEAD_BYTES_MAX + objectsLength);
    byte[] buffer = block.getBytes();
    objectsOffset = BinaryData.encodeLong(objectCount, buffer, 0);
    objectsOffset += BinaryData.encodeInt(objectsLength, buffer, objectsOffset);
    block.setSize(objectsOffset + objectsLength + AvroContainerFileHeader.SYNC_MARKER.length);
  }

  private void finalizeBuffer() {
    System.arraycopy(
      AvroContainerFileHeader.SYNC_MARKER, 0,
      block.getBytes(), objectsOffset + objectsLength,
      AvroContainerFileHeader.SYNC_MARKER.length);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof AvroContainerFileBlock)) return false;
    AvroContainerFileBlock that = (AvroContainerFileBlock) o;
    return block.equals(that.block);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return block.hashCode();
  }
}
