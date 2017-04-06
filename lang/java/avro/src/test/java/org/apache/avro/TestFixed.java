package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;

public class TestFixed {


  @Test
  public void testFixedDefaultValueDrop() {
    Schema md5 = SchemaBuilder.builder().fixed("MD5").size(16);
    Schema frec = SchemaBuilder.builder().record("test")
            .fields().name("hash").type(md5).withDefault(new byte[16]).endRecord();
    Schema.Field field = frec.getField("hash");
    Assert.assertNotNull(field.defaultVal());
    Assert.assertArrayEquals(new byte[16], (byte[]) field.defaultVal());
  }

}
