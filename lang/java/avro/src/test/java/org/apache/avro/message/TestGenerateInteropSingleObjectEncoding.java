package org.apache.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Generates <code>test_message.bin</code> - a
 * <a href="https://avro.apache.org/docs/current/spec.html#single_object_encoding">single object encoded</a>
 * Avro message.
 */
public class TestGenerateInteropSingleObjectEncoding {
  private static final String RESOURCES_FOLDER = System.getProperty("share.dir", "../../../share") + "/test/data/messageV1";
  private static final File SCHEMA_FILE = new File(RESOURCES_FOLDER + "/test_schema.json");
  private static final File MESSAGE_FILE = new File(RESOURCES_FOLDER + "/test_message.bin");
  private static Schema SCHEMA;
  private static GenericRecordBuilder BUILDER;


  @BeforeClass
  public static void setup() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(SCHEMA_FILE)) {
      SCHEMA = new Schema.Parser().parse(fileInputStream);
      BUILDER = new GenericRecordBuilder(SCHEMA);
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
