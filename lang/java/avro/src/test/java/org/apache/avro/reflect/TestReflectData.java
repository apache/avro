package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.junit.Test;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class TestReflectData {
  @Test
  @SuppressWarnings("unchecked")
  public void testWeakSchemaCaching() throws Exception {
    int numSchemas = 1000000;
    for (int i = 0; i < numSchemas; i++) {
      // Create schema
      Schema schema = Schema.createRecord("schema", null, null, false);
      schema.setFields(Collections.<Schema.Field>emptyList());

      ReflectData.get().getRecordState(new Object(), schema);
    }

    // Reflect the number of schemas currently in the cache
    ReflectData.ClassAccessorData classData = ReflectData.ACCESSOR_CACHE
        .get(Object.class);

    System.gc(); // Not guaranteed, but seems to be reliable enough

    assertThat("ReflectData cache should release references",
        classData.bySchema.size(), lessThan(numSchemas));
  }
}
