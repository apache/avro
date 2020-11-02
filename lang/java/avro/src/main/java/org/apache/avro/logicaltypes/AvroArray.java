package org.apache.avro.logicaltypes;

import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;

public class AvroArray implements AvroPrimitive {
  public static final String NAME = "ARRAY";
  public static final String TYPENAME = NAME;
  private static AvroArray element = new AvroArray();

  public AvroArray() {
    super();
  }

  public static AvroArray create() {
    return element;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Collection<?> convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof List) {
      return (Collection<?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a List");
  }

  @Override
  public Collection<?> convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Collection) {
      return (Collection<?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a List");
  }

  @Override
  public void toString(StringBuffer b, Object value) {
    if (value != null) {
      if (value instanceof List) {
        List<?> l = (List<?>) value;
        if (l.size() > 0) {
          b.append('[');
          boolean first = true;
          Object row1 = l.get(0);
          if (row1 instanceof Record) {
            AvroRecord recorddatatype = AvroRecord.create();
            for (Object o : l) {
              if (!first) {
                b.append(',');
              } else {
                first = false;
              }
              recorddatatype.toString(b, o);
            }
          } else {
            // This is not a perfect implementation as it renders the base Avro datatype
            // only, e.g ByteBuffer and not BigDecimal
            for (Object o : l) {
              if (!first) {
                b.append(',');
              } else {
                first = false;
              }
              b.append(o.toString());
            }
          }
          b.append(']');
        }
      }
    }
  }

  @Override
  public Type getBackingType() {
    return Type.ARRAY;
  }

  @Override
  public Schema getRecommendedSchema() {
    return null;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROARRAY;
  }

  @Override
  public Class<?> getConvertedType() {
    return Collection.class;
  }

}
