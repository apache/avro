package org.apache.avro.codegentest;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

abstract class AbstractSpecificRecordTest {

    <T extends SpecificRecordBase> void verifySerDeAndStandardMethods(T original) {
        final byte[] serialized = serialize(original);
        final T copy = deserialize(serialized, original.getSchema());
        Assert.assertEquals(original, copy);
        // In addition to equals() tested above, make sure the other methods that use SpecificData work as intended
        Assert.assertEquals(0, original.compareTo(copy));
        Assert.assertEquals(original.hashCode(), copy.hashCode());
        Assert.assertEquals(original.toString(), copy.toString());
    }

    private <T extends SpecificRecordBase> byte[] serialize(T object) {
        SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(object.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            datumWriter.write(object, EncoderFactory.get().directBinaryEncoder(outputStream, null));
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T extends SpecificRecordBase> T deserialize(byte[] bytes, Schema schema) {
        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(schema, schema);
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            return datumReader.read(null, DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
