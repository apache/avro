package com.ryonday.avro.test.v180;

import com.ryonday.avro.test.enums.ChildGameState;
import com.ryonday.avro.test.enums.EnumTest;
import com.ryonday.avro.test.enums.PiggyType;
import com.ryonday.avro.test.util.EncodeDecodeHelper;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.LoggerFactory.getLogger;

public class TestAvroEnumFail {

  private static final Logger logger = getLogger(AvroEnumFailTest.class);

  @Test
  public void testName() throws Exception {

    Integer a = new Integer(0);
    Long b = new Long(1231243134525L);
    List<Object> listError = Arrays.asList(a, b);
    logger.info("{}", listError);
    //error because Number&Comparable<?> is not Object
    List<Object> listObj = Arrays.<Object>asList(a, b);
    List<Number> listNum = Arrays.<Number>asList(a, b);
    List<Comparable<?>> listCmp = Arrays.<Comparable<?>>asList(a, b);

  }

  @Test
  public void genericDatumWriter_failsForSpecificRecord() throws Exception {
    EnumTest record = EnumTest.newBuilder()
      .setGameState(ChildGameState.Eeny)
      .setPiggyType(PiggyType.The_Piggy_Who_Stayed_Home)
      .build();

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToJsonGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToByteArrayGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThat(EncodeDecodeHelper.encodeToJsonSpecific(record)).isNotNull();

    assertThat(EncodeDecodeHelper.encodeToByteArraySpecific(record)).isNotNull();
  }

  @Test
  public void genericDatumWriter_failsForGenericRecord_populatedWithRawEnum() throws Exception {
    GenericRecord record = new GenericData.Record(EnumTest.getClassSchema());
    record.put("game_state", ChildGameState.Eeny);
    record.put("piggy_type", PiggyType.The_Piggy_Who_Stayed_Home);

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToJsonGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToByteArrayGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThat(EncodeDecodeHelper.encodeToJsonSpecific(record)).isNotNull();

    assertThat(EncodeDecodeHelper.encodeToByteArraySpecific(record)).isNotNull();
  }

  @Test
  public void genericDatumWriter_andSpecificDatumWriter_failForGenericRecord_populatedWithTextualEnum() throws Exception {
    GenericRecord record = new GenericData.Record(EnumTest.getClassSchema());
    record.put("game_state", "Eeny");
    record.put("piggy_type", "The_Piggy_Who_Stayed_Home");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToJsonGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToByteArrayGeneric(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToByteArraySpecific(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");

    assertThatThrownBy(() -> EncodeDecodeHelper.encodeToJsonSpecific(record))
      .isInstanceOf(AvroTypeException.class)
      .hasMessageStartingWith("Not an enum:");
  }

}
