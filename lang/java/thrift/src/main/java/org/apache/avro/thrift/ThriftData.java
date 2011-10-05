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
package org.apache.avro.thrift;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;

/** Utilities for serializing Thrift data in Avro format. */
public class ThriftData extends GenericData {
  static final String THRIFT_TYPE = "thrift";
  static final String THRIFT_PROP = "thrift";

  private static final ThriftData INSTANCE = new ThriftData();

  protected ThriftData() {}
  
  /** Return the singleton instance. */
  public static ThriftData get() { return INSTANCE; }

  @Override
  public void setField(Object r, String n, int pos, Object o) {
    setField(r, n, pos, o, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  public Object getField(Object r, String name, int pos) {
    return getField(r, name, pos, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  protected void setField(Object r, String n, int pos, Object v, Object state) {
    ((TBase)r).setFieldValue(((TFieldIdEnum[])state)[pos], v);
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    return ((TBase)record).getFieldValue(((TFieldIdEnum[])state)[pos]);
  }

  private final Map<Schema,TFieldIdEnum[]> fieldCache =
    new ConcurrentHashMap<Schema,TFieldIdEnum[]>();

  @Override
  protected Object getRecordState(Object r, Schema s) {
    TFieldIdEnum[] fields = fieldCache.get(s);
    if (fields == null) {                           // cache miss
      fields = new TFieldIdEnum[s.getFields().size()];
      Class c = r.getClass();
      for (TFieldIdEnum f : FieldMetaData.getStructMetaDataMap(c).keySet())
        fields[s.getField(f.getFieldName()).pos()] = f;
      fieldCache.put(s, fields);                  // update cache
    }
    return fields;
  }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof TBase && !(datum instanceof TUnion);
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    try {
      Class c = Class.forName(SpecificData.getClassName(schema));
      if (c == null)
        return newRecord(old, schema);            // punt to generic
      if (c.isInstance(old))
        return old;                               // reuse instance
      return c.newInstance();                     // create new instance
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return getSchema(record.getClass());
  }

  private final Map<Class,Schema> schemaCache
    = new ConcurrentHashMap<Class,Schema>();

  /** Return a record schema given a thrift generated class. */
  public Schema getSchema(Class c) {
    Schema schema = schemaCache.get(c);

    if (schema == null) {                         // cache miss
      try {
        schema = Schema.createRecord(c.getName(), null, null,
                                     Throwable.class.isAssignableFrom(c));
        List<Field> fields = new ArrayList<Field>();
        for (FieldMetaData f : FieldMetaData.getStructMetaDataMap(c).values()) {
          Schema s = getSchema(f.valueMetaData);
          fields.add(new Field(f.fieldName, s, null, null));
        }
        schema.setFields(fields);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      schemaCache.put(c, schema);                 // update cache
    }
    return schema;
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);

  private Schema getSchema(FieldValueMetaData f) {
    Schema result;
    switch (f.type) {
    case TType.BOOL:
      return Schema.create(Schema.Type.BOOLEAN);
    case TType.BYTE:
      Schema b = Schema.create(Schema.Type.INT);
      b.addProp(THRIFT_PROP, "byte");
      return b;
    case TType.I16:
      Schema s = Schema.create(Schema.Type.INT);
      s.addProp(THRIFT_PROP, "short");
      return s;
    case TType.I32:
      return Schema.create(Schema.Type.INT);
    case TType.I64:
      return Schema.create(Schema.Type.LONG);
    case TType.DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case TType.ENUM:
      EnumMetaData enumMeta = (EnumMetaData)f;
      Class<? extends Enum> c = (Class<? extends Enum>)enumMeta.enumClass;
      List<String> symbols = new ArrayList<String>();
      for (Enum e : c.getEnumConstants())
        symbols.add(e.name());
      return Schema.createEnum(c.getName(), null, null, symbols);
    case TType.LIST:
      ListMetaData listMeta = (ListMetaData)f;
      return Schema.createArray(getSchema(listMeta.elemMetaData));
    case TType.MAP:
      MapMetaData mapMeta = (MapMetaData)f;
      if (mapMeta.keyMetaData.type != TType.STRING)
        throw new AvroRuntimeException("Map keys must be strings: "+f);
      return Schema.createMap(getSchema(mapMeta.valueMetaData));
    case TType.SET:
      SetMetaData setMeta = (SetMetaData)f;
      Schema set = Schema.createArray(getSchema(setMeta.elemMetaData));
      set.addProp(THRIFT_PROP, "set");
      return set;
    case TType.STRING:
      if (f.isBinary())
        return Schema.create(Schema.Type.BYTES);
      else
        return Schema.create(Schema.Type.STRING);
    case TType.STRUCT:
      StructMetaData structMeta = (StructMetaData)f;
      Schema record = getSchema(structMeta.structClass);
      return record;
    case TType.VOID:
      return Schema.create(Schema.Type.NULL);
    default:
      throw new RuntimeException("Unexpected type in field: "+f);
    }
  }

}
