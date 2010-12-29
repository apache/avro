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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.conf.Configured;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

/** The {@link Serialization} used by jobs configured with {@link AvroJob}. */
public class AvroSerialization<T> extends Configured 
  implements Serialization<AvroWrapper<T>> {

  public boolean accept(Class<?> c) {
    return AvroWrapper.class.isAssignableFrom(c);
  }
  
  /** Returns the specified map output deserializer.  Defaults to the final
   * output deserializer if no map output schema was specified. */
  public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
    //  We need not rely on mapred.task.is.map here to determine whether map
    //  output or final output is desired, since the mapreduce framework never
    //  creates a deserializer for final output, only for map output.
    boolean isKey = AvroKey.class.isAssignableFrom(c);
    Schema schema = isKey
      ? Pair.getKeySchema(AvroJob.getMapOutputSchema(getConf()))
      : Pair.getValueSchema(AvroJob.getMapOutputSchema(getConf()));
    DatumReader<T> datumReader =
      getConf().getBoolean(AvroJob.MAP_OUTPUT_IS_REFLECT, false)
      ? new ReflectDatumReader<T>(schema)
      : new SpecificDatumReader<T>(schema);
    return new AvroWrapperDeserializer(datumReader, isKey);
  }
  
  private static final DecoderFactory FACTORY = new DecoderFactory();
  static { FACTORY.configureDirectDecoder(true); }

  private class AvroWrapperDeserializer
    implements Deserializer<AvroWrapper<T>> {

    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    private boolean isKey;
    
    public AvroWrapperDeserializer(DatumReader<T> reader, boolean isKey) {
      this.reader = reader;
      this.isKey = isKey;
    }
    
    public void open(InputStream in) {
      this.decoder = FACTORY.createBinaryDecoder(in, decoder);
    }
    
    public AvroWrapper<T> deserialize(AvroWrapper<T> wrapper)
      throws IOException {
      T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
      if (wrapper == null) {
        wrapper = isKey? new AvroKey<T>(datum) : new AvroValue<T>(datum);
      } else {
        wrapper.datum(datum);
      }
      return wrapper;
    }

    public void close() throws IOException {
      decoder.inputStream().close();
    }
    
  }
  
  /** Returns the specified output serializer. */
  public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
    // Here we must rely on mapred.task.is.map to tell whether the map output
    // or final output is needed.
    boolean isMap = getConf().getBoolean("mapred.task.is.map", false);
    Schema schema = !isMap
      ? AvroJob.getOutputSchema(getConf())
      : (AvroKey.class.isAssignableFrom(c)
         ? Pair.getKeySchema(AvroJob.getMapOutputSchema(getConf()))
         : Pair.getValueSchema(AvroJob.getMapOutputSchema(getConf())));
    return new AvroWrapperSerializer(new ReflectDatumWriter<T>(schema));
  }

  private class AvroWrapperSerializer implements Serializer<AvroWrapper<T>> {

    private DatumWriter<T> writer;
    private OutputStream out;
    private BinaryEncoder encoder;
    
    public AvroWrapperSerializer(DatumWriter<T> writer) {
      this.writer = writer;
    }

    public void open(OutputStream out) {
      this.out = out;
      this.encoder = new BinaryEncoder(out);
    }

    public void serialize(AvroWrapper<T> wrapper) throws IOException {
      writer.write(wrapper.datum(), encoder);
    }

    public void close() throws IOException {
      out.close();
    }

  }

}
