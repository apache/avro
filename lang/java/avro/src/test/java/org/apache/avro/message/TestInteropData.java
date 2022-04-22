package org.apache.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestInteropData {
  private static String inDir = System.getProperty("share.dir", "../../../share") + "/test/data/messageV1";
  private static File SCHEMA_FILE = new File(inDir + "/test_schema.json");
  private static File MESSAGE_FILE = new File(inDir + "/test_message.bin");
  private static final Schema SCHEMA;
  private static final GenericRecordBuilder BUILDER;

  static {
    try {
      SCHEMA = new Schema.Parser().parse(new FileInputStream(SCHEMA_FILE));
      BUILDER = new GenericRecordBuilder(SCHEMA);
    } catch (IOException e) {
      throw new RuntimeException("Interop Message Data Schema not found");
    }
  }

  @Test
  public void generateData() throws IOException {
    MessageEncoder<GenericData.Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA);
    BUILDER.set("id", 5L).set("name", "Bill").set("tags", Arrays.asList("dog_lover", "cat_hater")).build();
    ByteBuffer buffer = encoder
        .encode(BUILDER.set("id", 5L).set("name", "Bill").set("tags", Arrays.asList("dog_lover", "cat_hater")).build());
    new FileOutputStream(MESSAGE_FILE).write(buffer.array());
  }
}
