package org.apache.avro.specific;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.FooBarSpecificRecord;
import org.apache.avro.FooBarSpecificRecord.Builder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

public class TestSpecificDatumReader {

  public static byte[] serializeRecord(FooBarSpecificRecord fooBarSpecificRecord) throws IOException {
    SpecificDatumWriter<FooBarSpecificRecord> datumWriter = 
        new SpecificDatumWriter<FooBarSpecificRecord>(FooBarSpecificRecord.SCHEMA$);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    datumWriter.write(fooBarSpecificRecord, encoder);
    encoder.flush();
    return byteArrayOutputStream.toByteArray();
  }

  @Test
  public void testRead() throws IOException {
    Builder newBuilder = FooBarSpecificRecord.newBuilder();
    newBuilder.setId(42);
    newBuilder.setRelatedids(Arrays.asList(1,2,3));
    FooBarSpecificRecord specificRecord = newBuilder.build();
    
    byte[] recordBytes = serializeRecord(specificRecord);
    
    Decoder decoder = DecoderFactory.get().binaryDecoder(recordBytes, null);
    SpecificDatumReader<FooBarSpecificRecord> specificDatumReader = new SpecificDatumReader<FooBarSpecificRecord>(FooBarSpecificRecord.SCHEMA$);
    FooBarSpecificRecord deserialized = new FooBarSpecificRecord();
    specificDatumReader.read(deserialized, decoder);
    
    assertEquals(specificRecord, deserialized);
        
  }

}
