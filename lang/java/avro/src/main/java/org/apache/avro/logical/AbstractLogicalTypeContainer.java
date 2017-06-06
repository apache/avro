package org.apache.avro.logical;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

/**
 * Abstract container for LogicalType support
 */
public abstract class AbstractLogicalTypeContainer {

  /**
   * Return logical type factory
   *
   * @return
   */
  public abstract LogicalTypes.LogicalTypeFactory getFactory();

  /**
   * Return Conversion for logical type
   *
   * @return
   */
  public abstract Class<Conversion> getConversionClass();

  /**
   * Get type name
   */
  public abstract String getTypeName();

  /**
   * Performs integration with Avro ecosystem
   */
  public void register() {
    LogicalTypes.LogicalTypeFactory factory = getFactory();
    LogicalTypes.register(getTypeName(), factory);
    Conversion conversion = getConversion();
    GenericData.get().addLogicalTypeConversion(conversion);
    SpecificData.get().addLogicalTypeConversion(conversion);
  }

  public Conversion<?> getConversion() {
    try {
      return getConversionClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Unable to create Conversion");
    }
  }
}
