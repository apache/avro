package org.apache.avro.generic;

import org.codehaus.jackson.JsonNode;

/**
 * Interface that the encoding and decoding classes will use to find the default value for a field
 * @author zfarkas
 */
public interface DefaultValueProvider {

    JsonNode getCurrentFieldDefault();
  
}
