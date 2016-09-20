package org.apache.avro;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.avro.logicalTypes.BigIntegerFactory;
import org.apache.avro.logicalTypes.DecimalFactory;
import org.apache.avro.util.internal.JacksonUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.TextNode;

public abstract class AbstractLogicalType extends JsonProperties implements LogicalType {

  protected AbstractLogicalType(Schema.Type type, Set<String> reserved, String logicalTypeName,
          Map<String, Object> properties) {
    super(reserved);
    this.properties = new HashMap<String, Object>(properties);
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      props.put(prop.getKey(), JacksonUtils.toJsonNode(prop.getValue()));
    }
    this.properties.put("logicalType", logicalTypeName);
    props.put("logicalType", TextNode.valueOf(logicalTypeName));
    this.logicalTypeName = logicalTypeName;
    this.type = type;
  }

  protected final String logicalTypeName;

  protected final Map<String, Object> properties;

  protected final Schema.Type type;

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    AbstractLogicalType other = (AbstractLogicalType) obj;
    // equal if properties are the same
    return this.props.equals(other.props);
  }

  @Override
  public int hashCode() {
    return props.hashCode();
  }

  public String getLogicalTypeName() {
    return logicalTypeName;
  }

  public Object getProperty(String propertyName) {
    return properties.get(propertyName);
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  private static final Map<String, LogicalTypeFactory> LOGICAL_TYPE_TO_CLASS =
          new HashMap<String, LogicalTypeFactory>();

  private static final Logger LOG = Logger.getLogger(LogicalType.class.getName());

  static {

    ServiceLoader<LogicalTypeFactory> factories = ServiceLoader.load(LogicalTypeFactory.class);
    Iterator<LogicalTypeFactory> iterator = factories.iterator();
    while (iterator.hasNext()) {
      LogicalTypeFactory lf = iterator.next();
      LOGICAL_TYPE_TO_CLASS.put(lf.getLogicalTypeName(), lf);
    }
    DecimalFactory decimalFactory = new DecimalFactory();
    LOGICAL_TYPE_TO_CLASS.put(decimalFactory.getLogicalTypeName(), decimalFactory);
    BigIntegerFactory biFactory = new BigIntegerFactory();
    LOGICAL_TYPE_TO_CLASS.put(biFactory.getLogicalTypeName(), biFactory);
    }

  private static final ObjectMapper OM = new ObjectMapper();

  @Nullable
  public static LogicalType fromJsonNode(JsonNode node, Schema.Type schemaType) {
    final JsonNode logicalTypeNode = node.get("logicalType");
    if (logicalTypeNode == null) {
        return null;
    }
    LogicalTypeFactory factory = LOGICAL_TYPE_TO_CLASS.get(logicalTypeNode.asText());
    if (factory != null) {
        return factory.create(schemaType, OM.convertValue(node, Map.class));
    } else {
      if (Boolean.getBoolean("allowUndefinedLogicalTypes"))  {
        return null;
      } else {
        throw new RuntimeException("Undefined logical type " + logicalTypeNode.asText());
      }
    }
  }

  /** Helper method to build reserved property sets */
  public static Set<String> reservedSet(String... properties) {
    Set<String> reserved = new HashSet<String>();
    reserved.add("logicalType");
    Collections.addAll(reserved, properties);
    return reserved;
  }

}
