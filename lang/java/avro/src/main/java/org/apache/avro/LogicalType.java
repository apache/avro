package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

public class LogicalType {

  public static final String LOGICAL_TYPE_PROP = "logicalType";

  private static final String[] INCOMPATIBLE_PROPS = new String[] {
      GenericData.STRING_PROP, SpecificData.CLASS_PROP,
      SpecificData.KEY_CLASS_PROP, SpecificData.ELEMENT_PROP
  };

  public static final LogicalType UUID_TYPE = new LogicalType("uuid");

  private final String name;

  protected LogicalType(String logicalTypeName) {
    this.name = logicalTypeName;
  }

  public String getName() {
    return name;
  }

  /** Validate this logical type for the given Schema and add its props */
  public Schema addToSchema(Schema schema) {
    validate(schema);
    schema.addProp(LOGICAL_TYPE_PROP, name);
    return schema;
  }

  public void validate(Schema schema) {
    for (String incompatible : INCOMPATIBLE_PROPS) {
      if (schema.getProp(incompatible) != null) {
        throw new IllegalArgumentException(
            LOGICAL_TYPE_PROP + " cannot be used with " + incompatible);
      }
    }
  }

}
