package org.apache.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.AvroGenerated;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

@AvroGenerated
public class FullName extends SpecificRecordBase implements SpecificRecord {
  private static final long serialVersionUID = 4560514203639509981L;
  public static final Schema SCHEMA$ = (new Schema.Parser()).parse(
      "{\"type\":\"record\",\"name\":\"FullName\",\"namespace\":\"org.apache.avro\",\"fields\":[{\"name\":\"first\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"last\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");
  private String first;
  private String last;
  private static final DatumWriter WRITER$;
  private static final DatumReader READER$;
  static {
    WRITER$ = new SpecificDatumWriter(SCHEMA$);
    READER$ = new SpecificDatumReader(SCHEMA$);
  }

  public FullName() {
  }

  public FullName(String first, String last) {
    this.first = first;
    this.last = last;
  }

  public Schema getSchema() {
    return SCHEMA$;
  }

  public Object get(int field$) {
    switch (field$) {
    case 0:
      return this.first;
    case 1:
      return this.last;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  public void put(int field$, Object value$) {
    switch (field$) {
    case 0:
      this.first = (String) value$;
      break;
    case 1:
      this.last = (String) value$;
      break;
    default:
      throw new AvroRuntimeException("Bad index");
    }

  }

}
