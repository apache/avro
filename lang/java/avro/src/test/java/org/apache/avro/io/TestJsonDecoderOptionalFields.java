package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestJsonDecoderOptionalFields {
    static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"testSchema\",\n" +
            "  \"namespace\" : \"org.avro\",\n" +
            "  \"doc:\" : \"A basic schema for storing messages\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"username\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"Name of the user account\"\n" +
            "  }, {\n" +
            "    \"name\" : \"message\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"The content of the user's message\"\n" +
            "  }, {\n" +
            "    \"name\" : \"__timestamp\",\n" +
            "    \"type\" : [\"null\", \"long\"],\n" +
            "    \"doc\" : \"Epoch time in milliseconds (UTC)\",\n" +
            "    \"default\": null\n" +
            "  }, {\n" +
            "    \"name\": \"__metadata\",\n" +
            "    \"type\": [\"null\",{\"type\": \"map\", \"values\": \"string\"}],\n" +
            "    \"default\": null\n" +
            "  }]\n" +
            "}");

    JsonConverter converter = new JsonConverter();

    @Test
    public void testAllFieldsProvided() throws IOException {
        // given
        String json = "{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":{\"long\":1234}," +
                "\"__metadata\":{\"map\":{}}" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals(json, new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testLastFieldMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"," +
                "    \"__timestamp\": {\"long\": 1234}\n" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":{\"long\":1234}," +
                "\"__metadata\":null" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testMiddleFieldMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"," +
                "    \"__metadata\": {\"map\": {\"yes\": \"123\"}}\n" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":null," +
                "\"__metadata\":{\"map\":{\"yes\":\"123\"}}" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testAllFieldsMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":null," +
                "\"__metadata\":null" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    class JsonConverter {
        public byte[] convertToAvro(byte[] data, Schema schema) throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
            writer.write(readRecord(data, schema), encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }

        private GenericData.Record readRecord(byte[] data, Schema schema) throws IOException {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(data));
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(schema);
            return reader.read(null, decoder);
        }

        public byte[] convertToJson(byte[] avro, Schema schema) throws IOException {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avro, null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, binaryDecoder);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            new GenericDatumWriter<GenericRecord>(schema).write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toByteArray();
        }
    }

}
