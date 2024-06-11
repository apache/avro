package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class RecordReadWriteUtil {

  static <T> T read(byte[] toDecode) throws IOException {
    DatumReader<T> datumReader = new ReflectDatumReader<>();
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      dataFileReader.hasNext();
      return dataFileReader.next();
    }
  }

  static <T> byte[] write(T custom) {
    Schema schema = ReflectData.get().getSchema(custom.getClass());

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, baos);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static <T> byte[] write(T custom, Class<?> asName) {
    var schema = ReflectData.get().getSchema(custom.getClass());

    var schemaAs = ReflectData.get().getSchema(asName);

    var fields = schema.getFields().stream()
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
        .toList();

    schemaAs = Schema.createRecord(schemaAs.getName(), schemaAs.getDoc(), schemaAs.getNamespace(), schemaAs.isError(),
        fields);

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schemaAs, baos);
      datumWriter.setSchema(schema);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
