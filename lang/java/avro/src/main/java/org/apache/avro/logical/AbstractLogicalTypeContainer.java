package org.apache.avro.logical;

import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;

/**
 * Abstract container for LogicalType support
 */
public abstract class AbstractLogicalTypeContainer implements LogicalTypeContainer {

  /**
   * Performs integration with Avro ecosystem
   */
  public void register() {
    LogicalTypes.LogicalTypeFactory factory = this.getFactory();
    LogicalTypes.register(getTypeName(), factory);
    GenericData.get().addLogicalTypeConversion(getConversion());
  }


}
