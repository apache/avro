package org.apache.avro;

/**
 * An interface for validating the compatibility of a single schema against
 * another.
 * <p>
 * What makes one schema compatible with another is not defined by the contract.
 * <p/>
 */
public interface SchemaValidationStrategy {

  /**
   * Validates that one schema is compatible with another.
   * 
   * @throws SchemaValidationException if the schemas are not compatible.
   */
  void validate(Schema toValidate, Schema existing)
      throws SchemaValidationException;

}
