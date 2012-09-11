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
package org.apache.trevni.avro;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.trevni.TestUtil;

/** Generates schema data as Java objects with random values. */
public class RandomData implements Iterable<Object> {
  private final Schema root;
  private final int count;

  public RandomData(Schema schema, int count) {
    this.root = schema;
    this.count = count;
  }
  
  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      private int n;
      private Random random = TestUtil.createRandom();
      public boolean hasNext() { return n < count; }
      public Object next() {
        n++;
        return generate(root, random, 0);
      }
      public void remove() { throw new UnsupportedOperationException(); }
    };
  }
  
  @SuppressWarnings(value="unchecked")
  private static Object generate(Schema schema, Random random, int d) {
    switch (schema.getType()) {
    case RECORD:
      GenericRecord record = new GenericData.Record(schema);
      for (Schema.Field field : schema.getFields())
        record.put(field.name(), generate(field.schema(), random, d+1));
      return record;
    case ENUM:
      List<String> symbols = schema.getEnumSymbols();
      return new GenericData.EnumSymbol
        (schema, symbols.get(random.nextInt(symbols.size())));
    case ARRAY:
      int length = (random.nextInt(5)+2)-d;
      GenericArray<Object> array =
        new GenericData.Array(length<=0?0:length, schema);
      for (int i = 0; i < length; i++)
        array.add(generate(schema.getElementType(), random, d+1));
      return array;
    case MAP:
      length = (random.nextInt(5)+2)-d;
      Map<Object,Object> map = new HashMap<Object,Object>(length<=0?0:length);
      for (int i = 0; i < length; i++) {
        map.put(TestUtil.randomString(random),
                generate(schema.getValueType(), random, d+1));
      }
      return map;
    case UNION:
      List<Schema> types = schema.getTypes();
      return generate(types.get(random.nextInt(types.size())), random, d);
    case FIXED:
      byte[] bytes = new byte[schema.getFixedSize()];
      random.nextBytes(bytes);
      return new GenericData.Fixed(schema, bytes);
    case STRING:  return TestUtil.randomString(random);
    case BYTES:   return TestUtil.randomBytes(random);
    case INT:     return random.nextInt();
    case LONG:    return random.nextLong();
    case FLOAT:   return random.nextFloat();
    case DOUBLE:  return random.nextDouble();
    case BOOLEAN: return random.nextBoolean();
    case NULL:    return null;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length != 3) {
      System.out.println("Usage: RandomData <schemafile> <outputfile> <count>");
      System.exit(-1);
    }
    Schema sch = Schema.parse(new File(args[0]));
    DataFileWriter<Object> writer =
      new DataFileWriter<Object>(new GenericDatumWriter<Object>())
      .create(sch, new File(args[1]));
    try {
      for (Object datum : new RandomData(sch, Integer.parseInt(args[2]))) {
        writer.append(datum);
      }
    } finally {
      writer.close();
    }
  }
}
