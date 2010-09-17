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
package org.apache.avro.mapred;

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.net.URI;
import java.lang.reflect.Type;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.FileReader;
import org.apache.avro.reflect.ReflectData;

/** A {@link FileReader} for sequence files. */
@SuppressWarnings(value="unchecked")
public class SequenceFileReader<K,V> implements FileReader<Pair<K,V>> {
  private SequenceFile.Reader reader;
  private Schema schema;
  private boolean ready = false;            // true iff done & key are current
  private boolean done = false;             // true iff at EOF
  private Writable key, spareKey, value;

  private Converter<K> keyConverter =
    new Converter<K>() { public K convert(Writable o) { return (K)o; } };

  private Converter<V> valConverter =
    new Converter<V>() { public V convert(Writable o) { return (V)o; } };

  public SequenceFileReader(File file) throws IOException {
    this(file.toURI(), new Configuration());
  }

  public SequenceFileReader(URI uri, Configuration c) throws IOException {
    this(new SequenceFile.Reader(FileSystem.get(uri, c),
                                 new Path(uri.toString()), c), c);
  }

  public SequenceFileReader(SequenceFile.Reader reader, Configuration conf) {
    this.reader = reader;
    this.schema =
      Pair.getPairSchema(WritableData.get().getSchema(reader.getKeyClass()),
                         WritableData.get().getSchema(reader.getValueClass()));
    this.key =
      (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    this.spareKey =
      (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    this.value =
      (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

    if (WRITABLE_CONVERTERS.containsKey(reader.getKeyClass()) )
      keyConverter = WRITABLE_CONVERTERS.get(reader.getKeyClass());
    if (WRITABLE_CONVERTERS.containsKey(reader.getValueClass()) )
      valConverter = WRITABLE_CONVERTERS.get(reader.getValueClass());
  }

  @Override public void close() throws IOException { reader.close(); }

  @Override public void remove() { throw new UnsupportedOperationException(); }

  @Override public Iterator<Pair<K,V>> iterator() { return this; }

  @Override public Schema getSchema() { return schema; }

  private void prepare() throws IOException {
    if (ready) return;
    this.done = !reader.next(key);
    ready = true;
  }

  @Override public boolean hasNext() {
    try {
      prepare();
      return !done;
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override public Pair<K,V> next() {
    try {
      return next(null);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override public Pair<K,V> next(Pair<K,V> reuse) throws IOException {
    prepare();
    if (!hasNext())
      throw new NoSuchElementException();

    Pair<K,V> result = reuse;
    if (result == null)
      result = new Pair<K,V>(schema);

    result.key(keyConverter.convert(key));
    reader.getCurrentValue(value);
    result.value(valConverter.convert(value));

    // swap key and spareKey
    Writable k = key;
    key = spareKey;
    spareKey = k;

    ready = false;

    return result;
  }

  @Override public void sync(long position) throws IOException {
    if (position > reader.getPosition())
      reader.sync(position);
    ready = false;
  }
  
  @Override public boolean pastSync(long position) throws IOException {
    return reader.getPosition() >= position && reader.syncSeen();
  }

  @Override public long tell() throws IOException {return reader.getPosition();}

  private static final Map<Type,Schema> WRITABLE_SCHEMAS =
    new HashMap<Type,Schema>();
  static {
    WRITABLE_SCHEMAS.put(NullWritable.class,
                         Schema.create(Schema.Type.NULL));
    WRITABLE_SCHEMAS.put(BooleanWritable.class,
                         Schema.create(Schema.Type.BOOLEAN));
    WRITABLE_SCHEMAS.put(IntWritable.class,
                         Schema.create(Schema.Type.INT));
    WRITABLE_SCHEMAS.put(LongWritable.class,
                         Schema.create(Schema.Type.LONG));
    WRITABLE_SCHEMAS.put(FloatWritable.class,
                         Schema.create(Schema.Type.FLOAT));
    WRITABLE_SCHEMAS.put(DoubleWritable.class,
                         Schema.create(Schema.Type.DOUBLE));
    WRITABLE_SCHEMAS.put(BytesWritable.class,
                         Schema.create(Schema.Type.BYTES));
    WRITABLE_SCHEMAS.put(Text.class,
                         Schema.create(Schema.Type.STRING));
  }

  private static class WritableData extends ReflectData {
    private static final WritableData INSTANCE = new WritableData();
    protected WritableData() {}
    
    /** Return the singleton instance. */
    public static WritableData get() { return INSTANCE; }

    @Override public Schema getSchema(java.lang.reflect.Type type) {
      if (WRITABLE_SCHEMAS.containsKey(type))
        return WRITABLE_SCHEMAS.get(type);
      else
        return super.getSchema(type);
    }
  }

  private interface Converter<T> {
    T convert(Writable o);
  }
  
  private static final Map<Type,Converter> WRITABLE_CONVERTERS =
    new HashMap<Type,Converter>();
  static {
    WRITABLE_CONVERTERS.put
      (NullWritable.class,
       new Converter<Void>() {
        public Void convert(Writable o) { return null; }
      });
    WRITABLE_CONVERTERS.put
      (BooleanWritable.class,
       new Converter<Boolean>() {
        public Boolean convert(Writable o) {return ((BooleanWritable)o).get();}
      });
    WRITABLE_CONVERTERS.put
      (IntWritable.class,
       new Converter<Integer>() {
        public Integer convert(Writable o) { return ((IntWritable)o).get(); }
      });
    WRITABLE_CONVERTERS.put
      (LongWritable.class,
       new Converter<Long>() {
        public Long convert(Writable o) { return ((LongWritable)o).get(); }
      });
    WRITABLE_CONVERTERS.put
      (FloatWritable.class,
       new Converter<Float>() {
        public Float convert(Writable o) { return ((FloatWritable)o).get(); }
      });
    WRITABLE_CONVERTERS.put
      (DoubleWritable.class,
       new Converter<Double>() {
        public Double convert(Writable o) { return ((DoubleWritable)o).get(); }
      });
    WRITABLE_CONVERTERS.put
      (BytesWritable.class,
       new Converter<ByteBuffer>() {
        public ByteBuffer convert(Writable o) {
          BytesWritable b = (BytesWritable)o;
          return ByteBuffer.wrap(b.getBytes(), 0, b.getLength());
        }
      });
    WRITABLE_CONVERTERS.put
      (Text.class,
       new Converter<String>() {
        public String convert(Writable o) { return o.toString(); }
      });
  }


}
