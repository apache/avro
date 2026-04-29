package org.apache.avro.file;

import org.apache.avro.NameValidator;
import org.apache.avro.Schema;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class SchemaCache {
  public abstract Schema getOrParseSchema(String metaString);

  protected Schema parse(String metaString) {
    return new Schema.Parser(NameValidator.NO_VALIDATION).setValidateDefaults(false)
        .parse(metaString);
  }

  public static final SchemaCache NO_CACHE = new SchemaCache() {
    @Override
    public Schema getOrParseSchema(String metaString) {
      return parse(metaString);
    }
  };
  public static SchemaCache createConcurrentCache() {
    return new SchemaCache() {
      private final ConcurrentMap<String, Schema> cache = new ConcurrentHashMap<>();

      @Override
      public Schema getOrParseSchema(String metaString) {
        return cache.computeIfAbsent(metaString, this::parse);
      }
    };
  }

}
