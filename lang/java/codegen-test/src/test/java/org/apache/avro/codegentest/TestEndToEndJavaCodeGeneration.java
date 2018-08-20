package org.apache.avro.codegentest;

import org.apache.avro.codegentest.testdata.UnionWithLogicalTypes;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;

public class TestEndToEndJavaCodeGeneration {

    @Test
    public void testWithNullValues() throws IOException {
        UnionWithLogicalTypes instanceOfGeneratedClass = UnionWithLogicalTypes.newBuilder()
                .setDateOrNull(null)
                .setStringOrNull("hello")
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final UnionWithLogicalTypes copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getDateOrNull(), copy.getDateOrNull());
        Assert.assertEquals(instanceOfGeneratedClass.getStringOrNull(), copy.getStringOrNull());
    }

    @Test
    public void testDate() throws IOException {
        UnionWithLogicalTypes instanceOfGeneratedClass = UnionWithLogicalTypes.newBuilder()
                .setDateOrNull(LocalDate.now())
                .setStringOrNull("hello")
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final UnionWithLogicalTypes copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getDateOrNull(), copy.getDateOrNull());
        Assert.assertEquals(instanceOfGeneratedClass.getStringOrNull(), copy.getStringOrNull());
    }

    @Test
    public void testDecimal() throws IOException {
        UnionWithLogicalTypes instanceOfGeneratedClass = UnionWithLogicalTypes.newBuilder()
                .setStringOrNull("hello")
                .setDecimalOrNull(BigDecimal.valueOf(123, 2))
                .build();
        final byte[] serialized = serialize(instanceOfGeneratedClass);
        final UnionWithLogicalTypes copy = deserialize(serialized);
        Assert.assertEquals(instanceOfGeneratedClass.getDecimalOrNull(), copy.getDecimalOrNull());
        Assert.assertEquals(instanceOfGeneratedClass.getStringOrNull(), copy.getStringOrNull());
    }

    private byte[] serialize(UnionWithLogicalTypes object) {
        SpecificDatumWriter<UnionWithLogicalTypes> datumWriter = new SpecificDatumWriter<>(UnionWithLogicalTypes.getClassSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            datumWriter.write(object, EncoderFactory.get().directBinaryEncoder(outputStream, null));
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private UnionWithLogicalTypes deserialize(byte[] bytes) {
        SpecificDatumReader<UnionWithLogicalTypes> datumReader = new SpecificDatumReader<>(UnionWithLogicalTypes.getClassSchema(), UnionWithLogicalTypes.getClassSchema());
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            return datumReader.read(null, DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
