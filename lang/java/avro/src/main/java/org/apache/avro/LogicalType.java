package org.apache.avro;

import java.util.Map;
import java.util.Set;

public interface LogicalType  {
  
  /** Validate this logical type for the given Schema */
  void validate(Schema schema);

  /** Return the set of properties that a reserved for this type */
  Set<String> reserved();

  Object getProperty(String propertyName);

  Map<String, Object> getProperties();

  /** get java type */
  Class<?> getLogicalJavaType();
  
  Object deserialize(Object object);
 
  Object serialize(Object object);            

  String getLogicalTypeName();

}
