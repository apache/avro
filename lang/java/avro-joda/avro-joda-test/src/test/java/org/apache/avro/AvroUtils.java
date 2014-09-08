/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public final class AvroUtils {

  private AvroUtils() {
  }

  public static <T extends SpecificRecord> T readAvroJson(final byte[] bin, final Class<T> clasz) {
    try {
      return readAvroJson(new ByteArrayInputStream(bin), clasz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends SpecificRecord> T readAvroJson(final InputStream input, final Class<T> clasz)
          throws IOException {
    T res;
    try {
      res = (T) clasz.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    readAvroJson(input, res);
    return res;
  }

  public static <T extends SpecificRecord> void readAvroJson(final InputStream input, final T res)
          throws IOException {
    @SuppressWarnings("unchecked")
    DatumReader<T> reader = new SpecificDatumReader<T>((Class<T>) res.getClass());
    Decoder decoder = DecoderFactory.get().jsonDecoder(
            res.getSchema(), input);
    reader.read(res, decoder);
  }

  public static <T extends SpecificRecord> T readAvroBin(final byte[] bin, final Class<T> clasz) {
    try {
      return readAvroBin(new ByteArrayInputStream(bin), clasz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends SpecificRecord> T readAvroBin(final InputStream input, final Class<T> clasz)
          throws IOException {
    T res;
    try {
      res = (T) clasz.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    readAvroBin(input, res);
    return res;
  }

  private static <T extends SpecificRecord> void readAvroBin(final InputStream input, final T res)
          throws IOException {
    @SuppressWarnings("unchecked")
    DatumReader<T> reader = new SpecificDatumReader<T>((Class<T>) res.getClass());
    DecoderFactory decoderFactory = DecoderFactory.get();
    Decoder decoder = decoderFactory.validatingDecoder(res.getSchema(), decoderFactory.binaryDecoder(input, null));
    reader.read(res, decoder);
  }

  public static <T extends SpecificRecord> byte[] writeAvroJson(final T req) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    try {
      writeAvroJson(bos, req);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }

  public static <T extends SpecificRecord> void writeAvroJson(final OutputStream out, final T req)
          throws IOException {
    @SuppressWarnings("unchecked")
    DatumWriter<T> writer = new SpecificDatumWriter<T>((Class<T>) req.getClass());
    Encoder encoder = EncoderFactory.get().jsonEncoder(req.getSchema(), out);
    writer.write(req, encoder);
    encoder.flush();
  }

  public static <T extends SpecificRecord> byte[] writeAvroBin(final T req) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    try {
      writeAvroBin(bos, req);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }

  public static <T extends SpecificRecord> void writeAvroBin(final OutputStream out, final T req)
          throws IOException {
    @SuppressWarnings("unchecked")
    DatumWriter<T> writer = new SpecificDatumWriter<T>((Class<T>) req.getClass());
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(req, encoder);
    encoder.flush();
  }

  public static <T extends SpecificRecord> T[] readAvroJsonArray(final byte[] bin, final Class<T> clasz) {
    try {
      return readAvroJsonArray(new ByteArrayInputStream(bin), clasz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T extends SpecificRecord> T[] readAvroJsonArray(final InputStream os, final Class<T> clasz)
          throws IOException {
    List<T> res = new ArrayList<T>();
    T temp;
    try {
      temp = (T) clasz.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    Schema schema = Schema.createArray(temp.getSchema());
    DatumReader<List<T>> reader = new SpecificDatumReader<List<T>>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, os);
    reader.read(res, decoder);
    return res.toArray((T[]) Array.newInstance(clasz, res.size()));
  }

  public static <T extends SpecificRecord> byte[] writeAvroJsonArray(final T[] req) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(32);
    try {
      writeAvroJsonArray(bos, req);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  public static <T extends SpecificRecord> void writeAvroJsonArray(final OutputStream os, final T[] req)
          throws IOException {
    Schema schema;
    if (req.length == 0) {
      T temp;
      try {
        temp = (T) req.getClass().getComponentType().newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      schema = Schema.createArray(temp.getSchema());
    } else {
      schema = Schema.createArray(req[0].getSchema());
    }
    DatumWriter<List<T>> writer = new SpecificDatumWriter<List<T>>(schema);
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    writer.write(Arrays.asList(req), encoder);
    encoder.flush();
  }

}
