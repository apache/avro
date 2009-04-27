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
package org.apache.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/** Generates schema data as Java objects with random values. */
public class RandomData implements Iterable<Object> {
  private final Schema root;
  private final long seed;
  private final int count;

  public RandomData(Schema schema, int count) {
    this(schema, count, System.currentTimeMillis());
  }

  public RandomData(Schema schema, int count, long seed) {
    this.root = schema;
    this.seed = seed;
    this.count = count;
  }
  
  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      private int n;
      private Random random = new Random(seed);
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
      for (Map.Entry<String, Schema> entry : schema.getFieldSchemas())
        record.put(entry.getKey(), generate(entry.getValue(), random, d+1));
      return record;
    case ARRAY:
      int length = (random.nextInt(5)+2)-d;
      GenericArray<Object> array = new GenericData.Array(length<=0?0:length);
      for (int i = 0; i < length; i++)
        array.add(generate(schema.getElementType(), random, d+1));
      return array;
    case MAP:
      length = (random.nextInt(5)+2)-d;
      Map<Object,Object> map = new HashMap<Object,Object>(length<=0?0:length);
      for (int i = 0; i < length; i++) {
        map.put(randomUtf8(random, 40),
                generate(schema.getValueType(), random, d+1));
      }
      return map;
    case UNION:
      List<Schema> types = schema.getTypes();
      return generate(types.get(random.nextInt(types.size())), random, d);
    case STRING:  return randomUtf8(random, 40);
    case BYTES:   return randomBytes(random, 40);
    case INT:     return random.nextInt();
    case LONG:    return random.nextLong();
    case FLOAT:   return random.nextFloat();
    case DOUBLE:  return random.nextDouble();
    case BOOLEAN: return random.nextBoolean();
    case NULL:    return null;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  private static Utf8 randomUtf8(Random rand, int maxLength) {
    Utf8 utf8 = new Utf8().setLength(rand.nextInt(maxLength));
    for (int i = 0; i < utf8.getLength(); i++) {
      utf8.getBytes()[i] = (byte)('a'+rand.nextInt('z'-'a'));
    }
    return utf8;
  }

  private static ByteBuffer randomBytes(Random rand, int maxLength) {
    ByteBuffer bytes = ByteBuffer.allocate(rand.nextInt(maxLength));
    bytes.limit(bytes.capacity());
    rand.nextBytes(bytes.array());
    return bytes;
  }

  public static void main(String[] args) throws Exception {
    if(args.length != 3) {
      System.out.println("Usage: RandomData <schemafile> <outputfile> <count>");
      System.exit(-1);
    }
    Schema sch = Schema.parse(new File(args[0]));
    DataFileWriter<Object> writer =
      new DataFileWriter<Object>(sch, 
          new FileOutputStream(new File(args[1]),false),
          new GenericDatumWriter<Object>());
    try {
      for (Object datum : new RandomData(sch, Integer.parseInt(args[2]))) {
        writer.append(datum);
      }
    } finally {
      writer.close();
    }
  }
}
