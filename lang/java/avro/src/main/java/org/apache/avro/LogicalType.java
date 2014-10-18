package org.apache.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;

public abstract class LogicalType extends JsonProperties {

  protected LogicalType(Set<String> reserved, String logicalTypeName) {
    super(reserved);
    props.put("logicalType", TextNode.valueOf(logicalTypeName.toLowerCase()));
    this.logicalTypeName = logicalTypeName;
  }

  protected final String logicalTypeName;
  
  
  /** Validate this logical type for the given Schema */
  public abstract void validate(Schema schema);

  /** Return the set of properties that a reserved for this type */
  public abstract Set<String> reserved();

  /** get java type */
  public abstract Class<?> getLogicalJavaType();
  
  public abstract Object deserialize(Schema.Type type, Object object);
 
  public abstract Object serialize(Schema.Type type, Object object);
            
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    LogicalType other = (LogicalType) obj;
    // equal if properties are the same
    return this.props.equals(other.props);
  }

  @Override
  public int hashCode() {
    return props.hashCode();
  }

  
  private static final Map<String, Constructor<? extends LogicalType>> LOGICAL_TYPE_TO_CLASS =
          new HashMap<String, Constructor<? extends LogicalType>>();
    
  
  private static final Logger LOG = Logger.getLogger(LogicalType.class.getName());
  
  static {
    try {
      final Constructor<Decimal> constructor = Decimal.class.getConstructor(JsonNode.class);
      constructor.setAccessible(true);
      LOGICAL_TYPE_TO_CLASS.put("decimal", constructor);
    } catch (NoSuchMethodException ex) {
      throw new RuntimeException(ex);
    } catch (SecurityException ex) {
      throw new RuntimeException(ex);
    }

    Enumeration<URL> logTypesResources = null;
    try {
      logTypesResources
              = LogicalType.class.getClassLoader().getResources("org/apache/avro/logical_types.properties");
    } catch (IOException ex) {
      LOG.log(Level.INFO, "No external logical types registered", ex);
    }
    if (logTypesResources != null) {
      Properties props = new Properties();
      while (logTypesResources.hasMoreElements()) {
        URL url = logTypesResources.nextElement();
        LOG.info("Loading logical type registrations from " + url);
        try {
          BufferedReader is
                  = new BufferedReader(new InputStreamReader(url.openStream(), Charset.forName("US-ASCII")));
          try {
            props.load(is);
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
              try {
                final Constructor<? extends LogicalType> constructor
                        = ((Class<? extends LogicalType>) Class.forName((String) entry.getValue())).
                        getConstructor(JsonNode.class);
                constructor.setAccessible(true);
                LOGICAL_TYPE_TO_CLASS.put((String) entry.getKey(), constructor);
              } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
              } catch (NoSuchMethodException ex) {
                throw new RuntimeException(ex);
              } catch (SecurityException ex) {
                throw new RuntimeException(ex);
              }
            }
          } finally {
            props.clear();
            is.close();
          }
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }
  
  public static LogicalType fromJsonNode(JsonNode node) {
    final JsonNode logicalTypeNode = node.get("logicalType");
    if (logicalTypeNode == null) {
        return null;
    }
    Constructor<? extends LogicalType> constr = LOGICAL_TYPE_TO_CLASS.get(logicalTypeNode.asText());
    if (constr != null) {
        try {
            return constr.newInstance(node);
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    } else {
      throw new RuntimeException("Undefined logical type " + logicalTypeNode.asText());
    }
  }

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  static Decimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  static Decimal decimal(int precision, int scale) {
    return new Decimal(precision, scale);
  }

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers  */
  public static class Decimal extends LogicalType {
    private static final Set<String> RESERVED = reservedSet("precision", "scale");
    
    
    private final MathContext mc;
    private final int scale;

    private Decimal(int precision, int scale) {
      super(RESERVED, "decimal");
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " precision: " +
            precision + " (must be positive)");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException("Invalid " + this.logicalTypeName + " scale: " +
            scale + " (greater than precision: " + precision + ")");
      }
      props.put("precision", IntNode.valueOf(precision));
      props.put("scale", IntNode.valueOf(scale)); 
      mc = new MathContext(precision, RoundingMode.HALF_EVEN);
      this.scale = scale;
    }

    public Decimal(JsonNode node) {
        this(node.get("precision").asInt(), node.get("scale").asInt());      
    }

    @Override
    public void validate(Schema schema) {
      // validate the type
      if (schema.getType() != Schema.Type.FIXED &&
          schema.getType() != Schema.Type.BYTES &&
          schema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(this.logicalTypeName + " must be backed by fixed or bytes");
      }
      int precision = mc.getPrecision();
      if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException(
            "fixed(" + schema.getFixedSize() + ") cannot store " +
                precision + " digits (max " + maxPrecision(schema) + ")");
      }
   }

    @Override
    public Set<String> reserved() {
      return RESERVED;
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES
              || schema.getType() == Schema.Type.STRING) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(          // convert double to long
            Math.floor(Math.log10(  // number of base-10 digits
                Math.pow(2, 8 * size - 1) - 1)  // max value stored
            ));
      } else {
        // not valid for any other type
        return 0;
      }
    }

    @Override
    public Class<?> getLogicalJavaType() {
        return BigDecimal.class;
    }

    @Override
    public Object deserialize(Schema.Type type, Object object) {
      switch (type) {
        case STRING:
          BigDecimal result = new BigDecimal(((CharSequence) object).toString(), mc);
          if (result.scale() > scale) {
                      // Rounding might be an option.
            // this will probably need to be made configurable in the future.
            throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
          }
          return result;
        case BYTES:
          //ByteBuffer buf = ByteBuffer.wrap((byte []) object);
          ByteBuffer buf = (ByteBuffer) object;
          buf.rewind();
          int lscale = buf.getInt();
          if (lscale > scale) {
                      // Rounding might be an option.
            // this will probably need to be made configurable in the future.
            throw new AvroRuntimeException("Received Decimal " + object + " is not compatible with scale " + scale);
          }
          byte[] unscaled = new byte[buf.remaining()];
          buf.get(unscaled);
          BigInteger unscaledBi = new BigInteger(unscaled);
          return new BigDecimal(unscaledBi, lscale);
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }

    }

    @Override
    public Object serialize(Schema.Type type, Object object) {
      switch (type) {
        case STRING:
          return object.toString();
        case BYTES:
          BigDecimal decimal = (BigDecimal) object;
          int scale = decimal.scale();
          byte[] unscaledValue = decimal.unscaledValue().toByteArray();
          ByteBuffer buf = ByteBuffer.allocate(4 + unscaledValue.length);
          buf.putInt(scale);
          buf.put(unscaledValue);
          buf.rewind();
          return buf;
        default:
          throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
      }
    }
  }

  /** Helper method to build reserved property sets */
  private static Set<String> reservedSet(String... properties) {
    Set<String> reserved = new HashSet<String>();
    reserved.add("logicalType");
    Collections.addAll(reserved, properties);
    return reserved;
  }

}
