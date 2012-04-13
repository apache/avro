/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.hadoop.io;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * The {@link org.apache.hadoop.io.serializer.Serialization} used by jobs configured with
 * {@link org.apache.avro.mapreduce.AvroJob}.
 *
 * @param <T> The Java type of the Avro data to serialize.
 */
public class AvroSerialization<T> extends Configured implements Serialization<AvroWrapper<T>> {
  /** Conf key for the writer schema of the AvroKey datum being serialized/deserialized. */
  private static final String CONF_KEY_WRITER_SCHEMA = "avro.serialization.key.writer.schema";

  /** Conf key for the reader schema of the AvroKey datum being serialized/deserialized. */
  private static final String CONF_KEY_READER_SCHEMA = "avro.serialization.key.reader.schema";

  /** Conf key for the writer schema of the AvroValue datum being serialized/deserialized. */
  private static final String CONF_VALUE_WRITER_SCHEMA = "avro.serialization.value.writer.schema";

  /** Conf key for the reader schema of the AvroValue datum being serialized/deserialized. */
  private static final String CONF_VALUE_READER_SCHEMA = "avro.serialization.value.reader.schema";

  /** {@inheritDoc} */
  @Override
  public boolean accept(Class<?> c) {
    return AvroKey.class.isAssignableFrom(c) || AvroValue.class.isAssignableFrom(c);
  }

  /**
   * Gets an object capable of deserializing the output from a Mapper.
   *
   * @param c The class to get a deserializer for.
   * @return A deserializer for objects of class <code>c</code>.
   */
  @Override
  public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
    Configuration conf = getConf();
    if (AvroKey.class.isAssignableFrom(c)) {
      return new AvroKeyDeserializer<T>(getKeyWriterSchema(conf), getKeyReaderSchema(conf));
    } else if (AvroValue.class.isAssignableFrom(c)) {
      return new AvroValueDeserializer<T>(getValueWriterSchema(conf), getValueReaderSchema(conf));
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
  }

  /**
   * Gets an object capable of serializing output from a Mapper.
   *
   * @param c The class to get a serializer for.
   * @return A serializer for objects of class <code>c</code>.
   */
  @Override
  public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
    Schema schema;
    if (AvroKey.class.isAssignableFrom(c)) {
      schema = getKeyWriterSchema(getConf());
    } else if (AvroValue.class.isAssignableFrom(c)) {
      schema = getValueWriterSchema(getConf());
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
    return new AvroSerializer<T>(schema);
  }

  /**
   * Adds the AvroSerialization scheme to the configuration, so SerializationFactory
   * instances constructed from the given configuration will be aware of it.
   *
   * @param conf The configuration to add AvroSerialization to.
   */
  public static void addToConfiguration(Configuration conf) {
    Collection<String> serializations = conf.getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      conf.setStrings("io.serializations",
          serializations.toArray(new String[serializations.size()]));
    }
  }

  /**
   * Sets the writer schema of the AvroKey datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @param schema The Avro key schema.
   */
  public static void setKeyWriterSchema(Configuration conf, Schema schema) {
    if (null == schema) {
      throw new IllegalArgumentException("Writer schema may not be null");
    }
    conf.set(CONF_KEY_WRITER_SCHEMA, schema.toString());
  }

  /**
   * Sets the reader schema of the AvroKey datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @param schema The Avro key schema.
   */
  public static void setKeyReaderSchema(Configuration conf, Schema schema) {
    conf.set(CONF_KEY_READER_SCHEMA, schema.toString());
  }

  /**
   * Sets the writer schema of the AvroValue datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @param schema The Avro value schema.
   */
  public static void setValueWriterSchema(Configuration conf, Schema schema) {
    if (null == schema) {
      throw new IllegalArgumentException("Writer schema may not be null");
    }
    conf.set(CONF_VALUE_WRITER_SCHEMA, schema.toString());
  }

  /**
   * Sets the reader schema of the AvroValue datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @param schema The Avro value schema.
   */
  public static void setValueReaderSchema(Configuration conf, Schema schema) {
    conf.set(CONF_VALUE_READER_SCHEMA, schema.toString());
  }

  /**
   * Gets the writer schema of the AvroKey datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @return The Avro key writer schema, or null if none was set.
   */
  public static Schema getKeyWriterSchema(Configuration conf) {
    String json = conf.get(CONF_KEY_WRITER_SCHEMA);
    return null == json ? null : Schema.parse(json);
  }

  /**
   * Gets the reader schema of the AvroKey datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @return The Avro key reader schema, or null if none was set.
   */
  public static Schema getKeyReaderSchema(Configuration conf) {
    String json = conf.get(CONF_KEY_READER_SCHEMA);
    return null == json ? null : Schema.parse(json);
  }

  /**
   * Gets the writer schema of the AvroValue datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @return The Avro value writer schema, or null if none was set.
   */
  public static Schema getValueWriterSchema(Configuration conf) {
    String json = conf.get(CONF_VALUE_WRITER_SCHEMA);
    return null == json ? null : Schema.parse(json);
  }

  /**
   * Gets the reader schema of the AvroValue datum that is being serialized/deserialized.
   *
   * @param conf The configuration.
   * @return The Avro value reader schema, or null if none was set.
   */
  public static Schema getValueReaderSchema(Configuration conf) {
    String json = conf.get(CONF_VALUE_READER_SCHEMA);
    return null == json ? null : Schema.parse(json);
  }
}
