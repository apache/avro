/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.reflect;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificData;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import com.thoughtworks.paranamer.CachingParanamer;
import com.thoughtworks.paranamer.Paranamer;

/** Utilities to use existing Java classes and interfaces via reflection. */
public class ReflectData extends SpecificData {
  /** {@link ReflectData} implementation that permits null field values.  The
   * schema generated for each field is a union of its declared type and
   * null. */
  public static class AllowNull extends ReflectData {

    private static final AllowNull INSTANCE = new AllowNull();

    /** Return the singleton instance. */
    public static AllowNull get() { return INSTANCE; }

    @Override
    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
      Schema schema = super.createFieldSchema(field, names);
      return makeNullable(schema);
    }
  }
  
  private static final ReflectData INSTANCE = new ReflectData();

  public ReflectData() {}
  
  /** Construct with a particular classloader. */
  public ReflectData(ClassLoader classLoader) {
    super(classLoader);
  }
  
  /** Return the singleton instance. */
  public static ReflectData get() { return INSTANCE; }

  /** Cause a class to be treated as though it had an {@link Stringable}
   ** annotation. */
  public ReflectData addStringable(Class c) {
    stringableClasses.add(c);
    return this;
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new ReflectDatumReader(schema, schema, this);
  }

  @Override
  public void setField(Object record, String name, int position, Object o) {
    setField(record, name, position, o, null);
  }

  @Override
  protected void setField(Object record, String name, int pos, Object o,
    Object state) {
    if (record instanceof IndexedRecord) {
      super.setField(record, name, pos, o);
      return;
    }
    try {
      getAccessorForField(record, name, pos, state).set(record, o);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public Object getField(Object record, String name, int position) {
    return getField(record, name, position, null);
  }
  
  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    if (record instanceof IndexedRecord) {
      return super.getField(record, name, pos);
    }
    try {
      return getAccessorForField(record, name, pos, state).get(record);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }
    
  private FieldAccessor getAccessorForField(Object record, String name,
      int pos, Object optionalState) {
    if (optionalState != null) {
      return ((FieldAccessor[])optionalState)[pos];
    }
    return getFieldAccessor(record.getClass(), name);
  }

  @Override
  protected boolean isRecord(Object datum) {
    if (datum == null) return false;
    if (super.isRecord(datum)) return true;
    if (datum instanceof Collection) return false;
    if (datum instanceof Map) return false;
    return getSchema(datum.getClass()).getType() == Schema.Type.RECORD;
  }

  @Override
  protected boolean isArray(Object datum) {
    if (datum == null) return false;
    return (datum instanceof Collection) || datum.getClass().isArray();
  }

  @Override
  protected boolean isBytes(Object datum) {
    if (datum == null) return false;
    if (super.isBytes(datum)) return true;
    Class c = datum.getClass();
    return c.isArray() && c.getComponentType() == Byte.TYPE;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    if (record instanceof GenericContainer)
      return super.getRecordSchema(record);
    return getSchema(record.getClass());
  }

  @Override
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case ARRAY:
      if (!datum.getClass().isArray())
        return super.validate(schema, datum);
      int length = java.lang.reflect.Array.getLength(datum);
      for (int i = 0; i < length; i++)
        if (!validate(schema.getElementType(),
                      java.lang.reflect.Array.get(datum, i)))
          return false;
      return true;
    default:
      return super.validate(schema, datum);
    }
  }
  
  private static final ConcurrentHashMap<Class<?>, ClassAccessorData> 
    ACCESSOR_CACHE = new ConcurrentHashMap<Class<?>, ClassAccessorData>();

  private static class ClassAccessorData {
    private final Class<?> clazz;
    private final Map<String, FieldAccessor> byName =
        new HashMap<String, FieldAccessor>();
    private final IdentityHashMap<Schema, FieldAccessor[]> bySchema =
        new IdentityHashMap<Schema, FieldAccessor[]>();
        
    private ClassAccessorData(Class<?> c) {
      clazz = c;
      for(Field f : getFields(c, false)) {
        FieldAccessor accessor = ReflectionUtil.getFieldAccess().getAccessor(f);
        byName.put(f.getName(), accessor);
      }
    }
    
    /** 
     * Return the field accessors as an array, indexed by the field
     * index of the given schema.
     */
    private synchronized FieldAccessor[] getAccessorsFor(Schema schema) {
      FieldAccessor[] result = bySchema.get(schema);
      if (result == null) {
        result = createAccessorsFor(schema);
        bySchema.put(schema, result);
      }
      return result;
    }

    private FieldAccessor[] createAccessorsFor(Schema schema) {
      List<Schema.Field> avroFields = schema.getFields();
      FieldAccessor[] result = new FieldAccessor[avroFields.size()];
      for(Schema.Field avroField : schema.getFields()) {
        result[avroField.pos()] = byName.get(avroField.name());
      }
      return result;
    }

    private FieldAccessor getAccessorFor(String fieldName) {
      FieldAccessor result = byName.get(fieldName);
      if (result == null) {
        throw new AvroRuntimeException(
            "No field named " + fieldName + " in: " + clazz);
      }
      return result;
    }
  }
  
  private ClassAccessorData getClassAccessorData(Class<?> c) {
    ClassAccessorData data = ACCESSOR_CACHE.get(c);
    if(data == null && !IndexedRecord.class.isAssignableFrom(c)){
      ClassAccessorData newData = new ClassAccessorData(c);
      data = ACCESSOR_CACHE.putIfAbsent(c, newData);
      if (null == data) {
        data = newData;
      }
    }
    return data;
  }
  
  private FieldAccessor[] getFieldAccessors(Class<?> c, Schema s) {
    ClassAccessorData data = getClassAccessorData(c);
    if (data != null) {
      return data.getAccessorsFor(s);
    }
    return null;
  }
  
  private FieldAccessor getFieldAccessor(Class<?> c, String fieldName) {
    ClassAccessorData data = getClassAccessorData(c);
    if (data != null) {
      return data.getAccessorFor(fieldName);
    }
    return null;
  }

  /** @deprecated  Replaced by {@link SpecificData#CLASS_PROP} */
  @Deprecated
  static final String CLASS_PROP = "java-class";
  /** @deprecated  Replaced by {@link SpecificData#KEY_CLASS_PROP} */
  @Deprecated
  static final String KEY_CLASS_PROP = "java-key-class";
  /** @deprecated  Replaced by {@link SpecificData#ELEMENT_PROP} */
  @Deprecated
  static final String ELEMENT_PROP = "java-element-class";

  private static final Map<String,Class> CLASS_CACHE =
               new ConcurrentHashMap<String, Class>();

  static Class getClassProp(Schema schema, String prop) {
    String name = schema.getProp(prop);
    if (name == null) return null;
    Class c = CLASS_CACHE.get(name);
    if (c != null)
       return c;
    try {
      c = Class.forName(name);
      CLASS_CACHE.put(name, c);
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
    return c;
  }

  private static final Class BYTES_CLASS = new byte[0].getClass();
  private static final IdentityHashMap<Class, Class> ARRAY_CLASSES;
  static {
    ARRAY_CLASSES = new IdentityHashMap<Class, Class>();
    ARRAY_CLASSES.put(byte.class, byte[].class);
    ARRAY_CLASSES.put(char.class, char[].class);
    ARRAY_CLASSES.put(short.class, short[].class);
    ARRAY_CLASSES.put(int.class, int[].class);
    ARRAY_CLASSES.put(long.class, long[].class);
    ARRAY_CLASSES.put(float.class, float[].class);
    ARRAY_CLASSES.put(double.class, double[].class);
    ARRAY_CLASSES.put(boolean.class, boolean[].class);
  }

  @Override
  public Class getClass(Schema schema) {
    switch (schema.getType()) {
    case ARRAY:
      Class collectionClass = getClassProp(schema, CLASS_PROP);
      if (collectionClass != null)
        return collectionClass;
      Class elementClass = getClass(schema.getElementType());
      if(elementClass.isPrimitive()) {
        // avoid expensive translation to array type when primitive
        return ARRAY_CLASSES.get(elementClass);
      } else {
        return java.lang.reflect.Array.newInstance(elementClass, 0).getClass();
      }
    case STRING:
      Class stringClass = getClassProp(schema, CLASS_PROP);
      if (stringClass != null)
        return stringClass;
      return String.class;
    case BYTES:   return BYTES_CLASS;
    case INT:
      String intClass = schema.getProp(CLASS_PROP);
      if (Byte.class.getName().equals(intClass))  return Byte.TYPE;
      if (Short.class.getName().equals(intClass)) return Short.TYPE;
      if (Character.class.getName().equals(intClass)) return Character.TYPE;
    default:
      return super.getClass(schema);
    }
  }

  @Override
  protected Schema createSchema(Type type, Map<String,Schema> names) {
    if (type instanceof GenericArrayType) {                  // generic array
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component == Byte.TYPE)                            // byte array
        return Schema.create(Schema.Type.BYTES);           
      Schema result = Schema.createArray(createSchema(component, names));
      setElement(result, component);
      return result;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      Class raw = (Class)ptype.getRawType();
      Type[] params = ptype.getActualTypeArguments();
      if (Map.class.isAssignableFrom(raw)) {                 // Map
        Schema schema = Schema.createMap(createSchema(params[1], names));
        Class key = (Class)params[0];
        if (isStringable(key)) {                             // Stringable key
          schema.addProp(KEY_CLASS_PROP, key.getName());
        } else if (key != String.class) {
          throw new AvroTypeException("Map key class not String: "+key);
        }
        return schema;
      } else if (Collection.class.isAssignableFrom(raw)) {   // Collection
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        Schema schema = Schema.createArray(createSchema(params[0], names));
        schema.addProp(CLASS_PROP, raw.getName());
        return schema;
      }
    } else if ((type == Byte.class) || (type == Byte.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Byte.class.getName());
      return result;
    } else if ((type == Short.class) || (type == Short.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Short.class.getName());
      return result;
    } else if ((type == Character.class) || (type == Character.TYPE)) {
        Schema result = Schema.create(Schema.Type.INT);
        result.addProp(CLASS_PROP, Character.class.getName());
        return result;
    } else if (type instanceof Class) {                      // Class
      Class<?> c = (Class<?>)type;
      if (c.isPrimitive() ||                                 // primitives
          c == Void.class || c == Boolean.class || 
          c == Integer.class || c == Long.class ||
          c == Float.class || c == Double.class || 
          c == Byte.class || c == Short.class || 
          c == Character.class)
        return super.createSchema(type, names);
      if (c.isArray()) {                                     // array
        Class component = c.getComponentType();
        if (component == Byte.TYPE) {                        // byte array
          Schema result = Schema.create(Schema.Type.BYTES);
          result.addProp(CLASS_PROP, c.getName());
          return result;
        }
        Schema result = Schema.createArray(createSchema(component, names));
        result.addProp(CLASS_PROP, c.getName());
        setElement(result, component);
        return result;
      }
      if (CharSequence.class.isAssignableFrom(c))            // String
        return Schema.create(Schema.Type.STRING);
      if (ByteBuffer.class.isAssignableFrom(c))              // bytes
        return Schema.create(Schema.Type.BYTES);
      if (Collection.class.isAssignableFrom(c))              // array
        throw new AvroRuntimeException("Can't find element type of Collection");
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null) {
        String name = c.getSimpleName();
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        if (c.getEnclosingClass() != null)                   // nested class
          space = c.getEnclosingClass().getName() + "$";
        Union union = c.getAnnotation(Union.class);
        if (union != null) {                                 // union annotated
          return getAnnotatedUnion(union, names);
        } else if (isStringable(c)) {                        // Stringable
          Schema result = Schema.create(Schema.Type.STRING);
          result.addProp(CLASS_PROP, c.getName());
          return result;
        } else if (c.isEnum()) {                             // Enum
          List<String> symbols = new ArrayList<String>();
          Enum[] constants = (Enum[])c.getEnumConstants();
          for (int i = 0; i < constants.length; i++)
            symbols.add(constants[i].name());
          schema = Schema.createEnum(name, null /* doc */, space, symbols);
        } else if (GenericFixed.class.isAssignableFrom(c)) { // fixed
          int size = c.getAnnotation(FixedSize.class).value();
          schema = Schema.createFixed(name, null /* doc */, space, size);
        } else if (IndexedRecord.class.isAssignableFrom(c)) { // specific
          return super.createSchema(type, names);
        } else {                                             // record
          List<Schema.Field> fields = new ArrayList<Schema.Field>();
          boolean error = Throwable.class.isAssignableFrom(c);
          schema = Schema.createRecord(name, null /* doc */, space, error);
          names.put(c.getName(), schema);
          for (Field field : getCachedFields(c))
            if ((field.getModifiers()&(Modifier.TRANSIENT|Modifier.STATIC))==0){
              Schema fieldSchema = createFieldSchema(field, names);
              JsonNode defaultValue = null;
              if (fieldSchema.getType() == Schema.Type.UNION) {
                Schema defaultType = fieldSchema.getTypes().get(0);
                if (defaultType.getType() == Schema.Type.NULL) {
                  defaultValue = NullNode.getInstance();
                }
              }
              Schema.Field recordField = new Schema.Field(field.getName(),
                      fieldSchema, null /* doc */, defaultValue);
              fields.add(recordField);
            }
          if (error)                              // add Throwable message
            fields.add(new Schema.Field("detailMessage", THROWABLE_MESSAGE,
                                        null, null));
          schema.setFields(fields);
        }
        names.put(fullName, schema);
      }
      return schema;
    }
    return super.createSchema(type, names);
  }

  @Override protected boolean isStringable(Class<?> c) {
    return c.isAnnotationPresent(Stringable.class) || super.isStringable(c);
  }

  private static final Schema THROWABLE_MESSAGE =
    makeNullable(Schema.create(Schema.Type.STRING));

  // if array element type is a class with a union annotation, note it
  // this is required because we cannot set a property on the union itself 
  private void setElement(Schema schema, Type element) {
    if (!(element instanceof Class)) return;
    Class<?> c = (Class<?>)element;
    Union union = c.getAnnotation(Union.class);
    if (union != null)                          // element is annotated union
      schema.addProp(ELEMENT_PROP, c.getName());
  }

  // construct a schema from a union annotation
  private Schema getAnnotatedUnion(Union union, Map<String,Schema> names) {
    List<Schema> branches = new ArrayList<Schema>();
    for (Class branch : union.value())
      branches.add(createSchema(branch, names));
    return Schema.createUnion(branches);
  }

  /** Create and return a union of the null schema and the provided schema. */
  public static Schema makeNullable(Schema schema) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
                                            schema));
  }

  private static final Map<Class<?>,Field[]> FIELDS_CACHE =
    new ConcurrentHashMap<Class<?>,Field[]>();
  
  // Return of this class and its superclasses to serialize.
  private static Field[] getCachedFields(Class<?> recordClass) {
    Field[] fieldsList = FIELDS_CACHE.get(recordClass);
    if (fieldsList != null)
      return fieldsList;
    fieldsList = getFields(recordClass, true);
    FIELDS_CACHE.put(recordClass, fieldsList);
    return fieldsList;
  }

  private static Field[] getFields(Class<?> recordClass, boolean excludeJava) {
    Field[] fieldsList;
    Map<String,Field> fields = new LinkedHashMap<String,Field>();
    Class<?> c = recordClass;
    do {
      if (excludeJava && c.getPackage() != null
          && c.getPackage().getName().startsWith("java."))
        break;                                   // skip java built-in classes
      for (Field field : c.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0)
          if (fields.put(field.getName(), field) != null)
            throw new AvroTypeException(c+" contains two fields named: "+field);
      c = c.getSuperclass();
    } while (c != null);
    fieldsList = fields.values().toArray(new Field[0]);
    return fieldsList;
  }
  
  /** Create a schema for a field. */
  protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
    Schema schema = createSchema(field.getGenericType(), names);
    if (field.isAnnotationPresent(Nullable.class))           // nullable
      schema = makeNullable(schema);
    return schema;
  }

  /** Return the protocol for a Java interface.
   * <p>Note that this requires that <a
   * href="http://paranamer.codehaus.org/">Paranamer</a> is run over compiled
   * interface declarations, since Java 6 reflection does not provide access to
   * method parameter names.  See Avro's build.xml for an example. */
  @Override
  public Protocol getProtocol(Class iface) {
    Protocol protocol =
      new Protocol(iface.getSimpleName(),
                   iface.getPackage()==null?"":iface.getPackage().getName());
    Map<String,Schema> names = new LinkedHashMap<String,Schema>();
    Map<String,Message> messages = protocol.getMessages();
    for (Method method : iface.getMethods())
      if ((method.getModifiers() & Modifier.STATIC) == 0) {
        String name = method.getName();
        if (messages.containsKey(name))
          throw new AvroTypeException("Two methods with same name: "+name);
        messages.put(name, getMessage(method, protocol, names));
      }

    // reverse types, since they were defined in reference order
    List<Schema> types = new ArrayList<Schema>();
    types.addAll(names.values());
    Collections.reverse(types);
    protocol.setTypes(types);

    return protocol;
  }

  private final Paranamer paranamer = new CachingParanamer();

  private Message getMessage(Method method, Protocol protocol,
                             Map<String,Schema> names) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    String[] paramNames = paranamer.lookupParameterNames(method);
    Type[] paramTypes = method.getGenericParameterTypes();
    Annotation[][] annotations = method.getParameterAnnotations();
    for (int i = 0; i < paramTypes.length; i++) {
      Schema paramSchema = getSchema(paramTypes[i], names);
      for (int j = 0; j < annotations[i].length; j++)
        if (annotations[i][j] instanceof Union)
          paramSchema = getAnnotatedUnion(((Union)annotations[i][j]), names);
        else if (annotations[i][j] instanceof Nullable)
          paramSchema = makeNullable(paramSchema);
      String paramName =  paramNames.length == paramTypes.length
        ? paramNames[i]
        : paramSchema.getName()+i;
      fields.add(new Schema.Field(paramName, paramSchema,
        null /* doc */, null));
    }
    Schema request = Schema.createRecord(fields);

    Union union = method.getAnnotation(Union.class);
    Schema response = union == null
      ? getSchema(method.getGenericReturnType(), names)
      : getAnnotatedUnion(union, names);
    if (method.isAnnotationPresent(Nullable.class))          // nullable
      response = makeNullable(response);

    List<Schema> errs = new ArrayList<Schema>();
    errs.add(Protocol.SYSTEM_ERROR);              // every method can throw
    for (Type err : method.getGenericExceptionTypes())
      if (err != AvroRemoteException.class) 
        errs.add(getSchema(err, names));
    Schema errors = Schema.createUnion(errs);
    return protocol.createMessage(method.getName(), null /* doc */, request, response, errors);
  }

  private Schema getSchema(Type type, Map<String,Schema> names) {
    try {
      return createSchema(type, names);
    } catch (AvroTypeException e) {               // friendly exception
      throw new AvroTypeException("Error getting schema for "+type+": "
                                  +e.getMessage(), e);
    }
  }

  @Override
  protected int compare(Object o1, Object o2, Schema s, boolean equals) {
    switch (s.getType()) {
    case ARRAY:
      if (!o1.getClass().isArray())
        break;
      Schema elementType = s.getElementType();
      int l1 = java.lang.reflect.Array.getLength(o1);
      int l2 = java.lang.reflect.Array.getLength(o2);
      int l = Math.min(l1, l2);
      for (int i = 0; i < l; i++) {
        int compare = compare(java.lang.reflect.Array.get(o1, i),
                              java.lang.reflect.Array.get(o2, i),
                              elementType, equals);
        if (compare != 0) return compare;
      }
      return l1 - l2;
    case BYTES:
      if (!o1.getClass().isArray())
        break;
      byte[] b1 = (byte[])o1; 
      byte[] b2 = (byte[])o2; 
      return BinaryData.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
    }
    return super.compare(o1, o2, s, equals);
  }

  @Override
  protected Object getRecordState(Object record, Schema schema) {
    return getFieldAccessors(record.getClass(), schema);
  }
}
