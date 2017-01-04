package org.apache.avro.logical;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;

/**
 * Container for LogicalType support
 */
public interface LogicalTypeContainer {

  /**
   * Return logical type factory
   *
   * @return
   */
  LogicalTypes.LogicalTypeFactory getFactory();

  /**
   * Return Conversion for logical type
   *
   * @return
   */
  Conversion getConversion();

  /**
   * Get type name
   */
  String getTypeName();

  /**
   * Performs integration with Avro ecosystem
   */
  void register();

}
