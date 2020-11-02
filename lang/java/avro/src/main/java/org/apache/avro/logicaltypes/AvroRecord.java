package org.apache.avro.logicaltypes;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;

/**
 * Used to read Records.
 *
 */
public class AvroRecord implements AvroDatatype {
  public static final String NAME = "RECORD";
  static AvroRecord element = new AvroRecord();

  public AvroRecord() {
    super();
  }

  public static AvroRecord create() {
    return element;
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value instanceof Record) {
      Record r = (Record) value;
      Schema schema = r.getSchema();
      b.append('{');
      boolean first = true;
      for (Field f : schema.getFields()) {
        Object v = r.get(f.pos());
        if (v != null) {
          AvroDatatype datatype = AvroType.getBaseSchema(f.schema()).getDataType();
          if (datatype != null) {
            if (!first) {
              b.append(',');
            } else {
              first = false;
            }
            b.append('\"');
            b.append(f.name());
            b.append("\":");
            datatype.toString(b, v);
          }
        }
      }
      b.append('}');
    }
  }

  @Override
  public IndexedRecord convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof IndexedRecord) {
      return (IndexedRecord) value;
    }
    throw new AvroTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName()
        + "\" into a IndexedRecord (SpecificRecord, GenericRecord)");
  }

  @Override
  public IndexedRecord convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof IndexedRecord) {
      return (IndexedRecord) value;
    }
    throw new AvroTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName()
        + "\" into a IndexedRecord (SpecificRecord, GenericRecord)");
  }

  @Override
  public Type getBackingType() {
    return Type.RECORD;
  }

  @Override
  public Schema getRecommendedSchema() {
    return null;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVRORECORD;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Class<?> getConvertedType() {
    return IndexedRecord.class;
  }

}
