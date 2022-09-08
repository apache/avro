package org.apache.avro;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.FastReaderBuilder;
import org.apache.avro.io.JsonDecoder;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResolverTest {

  /**
   * Test promote action INT -> LONG, with logical type for LONG.
   */
  @Test
  void resolveTime() {
    final Schema writeSchema = Schema.create(Schema.Type.INT);
    final Schema readSchema = new TimeConversions.TimeMicrosConversion().getRecommendedSchema(); // LONG

    Resolver.Action action = Resolver.resolve(writeSchema, readSchema);
    Assertions.assertNotNull(action);
    MatcherAssert.assertThat("Wrong class for action", action, Matchers.instanceOf(Resolver.Promote.class));
    Assertions.assertEquals(action.type, Resolver.Action.Type.PROMOTE);
    Assertions.assertNotNull(action.logicalType);
  }

  /**
   * Test union type with promote action INT -> LONG, with logical type for LONG.
   */
  @Test
  void resolveUnion() {
    final Schema schema = new TimeConversions.TimeMicrosConversion().getRecommendedSchema();

    final Schema writeSchema = Schema.createUnion(Schema.create(Schema.Type.INT));
    final Schema readSchema = Schema.createUnion(schema);

    Resolver.Action action = Resolver.resolve(writeSchema, readSchema);
    Assertions.assertNotNull(action);
    Assertions.assertEquals(action.type, Resolver.Action.Type.WRITER_UNION);
    MatcherAssert.assertThat("Wrong class for action", action, Matchers.instanceOf(Resolver.WriterUnion.class));

    Assertions.assertEquals(1, ((Resolver.WriterUnion) action).actions.length);
    Resolver.Action innerAction = ((Resolver.WriterUnion) action).actions[0];

    MatcherAssert.assertThat("Wrong class for action", innerAction, Matchers.instanceOf(Resolver.ReaderUnion.class));
    Resolver.ReaderUnion innerUnionAction = (Resolver.ReaderUnion) innerAction;
    Resolver.Action promoteAction = innerUnionAction.actualAction;
    Assertions.assertEquals(promoteAction.type, Resolver.Action.Type.PROMOTE);
    Assertions.assertNotNull(promoteAction.logicalType);
  }

  @Test
  void resolveEnum() throws IOException {
    final Schema writeSchema = Schema.createEnum("myEnum", "", "n1", Arrays.asList("e1", "e3", "e4"));
    final Schema readSchema = Schema.createEnum("myEnum", "", "n1", Arrays.asList("e1", "e2", "e3"), "e2");

    Resolver.Action action = Resolver.resolve(writeSchema, readSchema);
    Assertions.assertNotNull(action);
    Assertions.assertEquals(action.type, Resolver.Action.Type.ENUM);
    MatcherAssert.assertThat("Wrong class for action", action, Matchers.instanceOf(Resolver.EnumAdjust.class));
    Resolver.EnumAdjust adjust = (Resolver.EnumAdjust) action;

    Assertions.assertArrayEquals(new int[] { 0, 2, 1 }, adjust.adjustments);
    Assertions.assertEquals("e1", adjust.values[0].toString());
    Assertions.assertEquals("e3", adjust.values[1].toString());
    Assertions.assertEquals("e2", adjust.values[2].toString());

    FastReaderBuilder reader = FastReaderBuilder.get();
    Schema writeRecord = Schema.createRecord("rec1", "", "", false,
        Arrays.asList(new Schema.Field("f1", writeSchema, "")));
    Schema readRecord = Schema.createRecord("rec1", "", "", false,
        Arrays.asList(new Schema.Field("f1", readSchema, "")));
    DatumReader<Object> datumReader = reader.createDatumReader(writeRecord, readRecord);
    JsonDecoder e2 = DecoderFactory.get().jsonDecoder(readRecord, "{ \"f1\" : \"e2\" }");
    Object read = datumReader.read(null, e2);
    Assertions.assertNotNull(read);
    MatcherAssert.assertThat("", read, Matchers.instanceOf(IndexedRecord.class));
    IndexedRecord result = (IndexedRecord) read;
    Assertions.assertEquals("e3", result.get(0).toString());
  }

  @Test
  void promoteIsValid() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> Resolver.Promote.isValid(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT)));

    Assertions.assertTrue(Resolver.Promote.isValid(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG)));
    Assertions.assertFalse(Resolver.Promote.isValid(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.INT)));

    Assertions.assertTrue(Resolver.Promote.isValid(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.FLOAT)));
    Assertions.assertFalse(Resolver.Promote.isValid(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.INT)));

    Assertions
        .assertTrue(Resolver.Promote.isValid(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.DOUBLE)));
    Assertions
        .assertFalse(Resolver.Promote.isValid(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.FLOAT)));
  }
}
