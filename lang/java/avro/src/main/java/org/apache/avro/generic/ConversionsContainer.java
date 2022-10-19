package org.apache.avro.generic;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConversionsContainer {

  private Map<String, Conversion<?>> conversions = new HashMap<>();

  public static class ClassConversions {

    private final Map<String, Conversion<?>> classConversions;

    public ClassConversions(final Map<String, Conversion<?>> conversions) {
      this.classConversions = conversions;
    }

    public boolean containsKey(String name) {
      return classConversions.containsKey(name);
    }

    public Conversion<?> get(String name) {
      return classConversions.get(name);
    }
  }

  private Map<Class<?>, Map<String, Conversion<?>>> conversionsByClass = new IdentityHashMap<>();

  public Collection<Conversion<?>> getConversions() {
    return conversions.values();
  }

  /**
   * Registers the given conversion to be used when reading and writing with this
   * data model.
   *
   * @param conversion a logical type Conversion.
   */
  public void addLogicalTypeConversion(final Conversion<?> conversion) {
    this.conversions.put(conversion.getLogicalTypeName(), conversion);
    final Class<?> type = conversion.getConvertedType();
    this.conversionsByClass.compute(type, (Class<?> key, Map<String, Conversion<?>> v) -> {
      final Map<String, Conversion<?>> conv = v != null ? v : new LinkedHashMap<>();
      conv.put(conversion.getLogicalTypeName(), conversion);
      return conv;
    });
  }

  /**
   * Returns the first conversion found for the given class.
   *
   * @param datumClass a Class
   * @return the first registered conversion for the class, or null
   */
  @SuppressWarnings("unchecked")
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass) {
    Map<String, Conversion<?>> conversions = conversionsByClass.get(datumClass);
    if (conversions != null) {
      return (Conversion<T>) conversions.values().iterator().next();
    }
    return null;
  }

  /**
   * Returns the conversion for the given class and logical type.
   *
   * @param datumClass  a Class
   * @param logicalType a LogicalType
   * @return the conversion for the class and logical type, or null
   */
  @SuppressWarnings("unchecked")
  public <T> Conversion<T> getConversionByClass(Class<T> datumClass, LogicalType logicalType) {
    Map<String, Conversion<?>> conversions = conversionsByClass.get(datumClass);
    if (conversions != null) {
      return (Conversion<T>) conversions.get(logicalType.getName());
    }
    return null;
  }

  /**
   * Returns the Conversion for the given logical type.
   *
   * @param logicalType a logical type
   * @return the conversion for the logical type, or null
   */
  @SuppressWarnings("unchecked")
  public Conversion<Object> getConversionFor(LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    }
    return (Conversion<Object>) conversions.get(logicalType.getName());
  }

  public ClassConversions forClass(Class<?> clazz) {
    final Map<String, Conversion<?>> conversionMap = this.conversionsByClass.get(clazz);
    if (conversionMap == null) {
      return null;
    }
    return new ClassConversions(conversionMap);
  }
}
