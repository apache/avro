/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.generic;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collection;

public class PrimitivesArrays {

  public static class IntArray extends GenericData.AbstractArray<Integer> {
    private static final int[] EMPTY = new int[0];

    private int[] elements = EMPTY;

    public IntArray(int capacity, Schema schema) {
      super(schema);
      if (!Schema.Type.INT.equals(schema.getElementType().getType()))
        throw new AvroRuntimeException("Not a int array schema: " + schema);
      if (capacity != 0)
        elements = new int[capacity];
    }

    public IntArray(Schema schema, Collection<Integer> c) {
      super(schema);
      if (c != null) {
        elements = new int[c.size()];
        addAll(c);
      }
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public Integer get(int i) {
      return this.getInt(i);
    }

    /**
     * Direct primitive int access.
     * 
     * @param i : index.
     * @return value at index.
     */
    public int getInt(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return elements[i];
    }

    @Override
    public void add(int location, Integer o) {
      if (o == null) {
        return;
      }
      this.add(location, o.intValue());
    }

    public void add(int location, int o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length) {
        // Increase size by 1.5x + 1
        final int newSize = size + (size >> 1) + 1;
        elements = Arrays.copyOf(elements, newSize);
      }
      System.arraycopy(elements, location, elements, location + 1, size - location);
      elements[location] = o;
      size++;
    }

    @Override
    public Integer set(int i, Integer o) {
      if (o == null) {
        return null;
      }
      return this.set(i, o.intValue());
    }

    public int set(int i, int o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      int response = elements[i];
      elements[i] = o;
      return response;
    }

    @Override
    public Integer remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      int result = elements[i];
      --size;
      System.arraycopy(elements, i + 1, elements, i, (size - i));
      return result;
    }

    @Override
    public Integer peek() {
      return (size < elements.length) ? elements[size] : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      int tmp = elements[index1];
      elements[index1] = elements[index2];
      elements[index2] = tmp;
    }
  }

  public static class LongArray extends GenericData.AbstractArray<Long> {
    private static final long[] EMPTY = new long[0];

    private long[] elements = EMPTY;

    public LongArray(int capacity, Schema schema) {
      super(schema);
      if (!Schema.Type.LONG.equals(schema.getElementType().getType()))
        throw new AvroRuntimeException("Not a long array schema: " + schema);
      if (capacity != 0)
        elements = new long[capacity];
    }

    public LongArray(Schema schema, Collection<Long> c) {
      super(schema);
      if (c != null) {
        elements = new long[c.size()];
        addAll(c);
      }
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public Long get(int i) {
      return getLong(i);
    }

    /**
     * Direct primitive int access.
     * 
     * @param i : index.
     * @return value at index.
     */
    public long getLong(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return elements[i];
    }

    @Override
    public void add(int location, Long o) {
      if (o == null) {
        return;
      }
      this.add(location, o.longValue());
    }

    public void add(int location, long o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length) {
        // Increase size by 1.5x + 1
        final int newSize = size + (size >> 1) + 1;
        elements = Arrays.copyOf(elements, newSize);
      }
      System.arraycopy(elements, location, elements, location + 1, size - location);
      elements[location] = o;
      size++;
    }

    @Override
    public Long set(int i, Long o) {
      if (o == null) {
        return null;
      }
      return this.set(i, o.longValue());
    }

    public long set(int i, long o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      long response = elements[i];
      elements[i] = o;
      return response;
    }

    @Override
    public Long remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      long result = elements[i];
      --size;
      System.arraycopy(elements, i + 1, elements, i, (size - i));
      return result;
    }

    @Override
    public Long peek() {
      return (size < elements.length) ? elements[size] : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      long tmp = elements[index1];
      elements[index1] = elements[index2];
      elements[index2] = tmp;
    }
  }

  public static class BooleanArray extends GenericData.AbstractArray<Boolean> {
    private static final byte[] EMPTY = new byte[0];

    private byte[] elements = EMPTY;

    public BooleanArray(int capacity, Schema schema) {
      super(schema);
      if (!Schema.Type.BOOLEAN.equals(schema.getElementType().getType()))
        throw new AvroRuntimeException("Not a boolean array schema: " + schema);
      if (capacity != 0)
        elements = new byte[1 + (capacity / Byte.SIZE)];
    }

    public BooleanArray(Schema schema, Collection<Boolean> c) {
      super(schema);

      if (c != null) {
        elements = new byte[1 + (c.size() / 8)];
        if (c instanceof BooleanArray) {
          BooleanArray other = (BooleanArray) c;
          this.size = other.size;
          System.arraycopy(other.elements, 0, this.elements, 0, other.elements.length);
        } else {
          addAll(c);
        }
      }
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public Boolean get(int i) {
      return this.getBoolean(i);
    }

    /**
     * Direct primitive int access.
     * 
     * @param i : index.
     * @return value at index.
     */
    public boolean getBoolean(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return (elements[i / 8] & (1 << (i % 8))) > 0;
    }

    @Override
    public boolean add(final Boolean o) {
      if (o == null) {
        return false;
      }
      return this.add(o.booleanValue());
    }

    public boolean add(final boolean o) {
      if (this.size == elements.length * 8) {
        final int newLength = elements.length + (elements.length >> 1) + 1;
        elements = Arrays.copyOf(elements, newLength);
      }
      this.size++;
      this.set(this.size - 1, o);
      return true;
    }

    @Override
    public void add(int location, Boolean o) {
      if (o == null) {
        return;
      }
      this.add(location, o.booleanValue());
    }

    public void add(int location, boolean o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length * 8) {
        // Increase size by 1.5x + 1
        final int newLength = elements.length + (elements.length >> 1) + 1;
        elements = Arrays.copyOf(elements, newLength);
      }
      size++;
      for (int index = this.size / 8; index > (location / 8); index--) {
        elements[index] <<= 1;
        if (index > 0 && (elements[index - 1] & (1 << Byte.SIZE)) > 0) {
          elements[index] |= 1;
        }
      }
      byte pos = (byte) (1 << (location % Byte.SIZE));
      byte highbits = (byte) ~(pos + (pos - 1));
      byte lowbits = (byte) (pos - 1);
      byte currentHigh = (byte) ((elements[location / 8] & highbits) << 1);
      byte currentLow = (byte) (elements[location / 8] & lowbits);
      if (o) {
        elements[location / 8] = (byte) (currentHigh | currentLow | pos);
      } else {
        elements[location / 8] = (byte) (currentHigh | currentLow);
      }

    }

    @Override
    public Boolean set(int i, Boolean o) {
      if (o == null) {
        return null;
      }
      return this.set(i, o.booleanValue());
    }

    public boolean set(int i, boolean o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      boolean response = (elements[i / 8] & (1 << (i % 8))) > 0;
      if (o) {
        elements[i / 8] |= 1 << (i % 8);
      } else {
        elements[i / 8] &= 0xFF - (1 << (i % 8));
      }
      return response;
    }

    @Override
    public Boolean remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      boolean result = (elements[(i / 8)] & (1 << (i % 8))) > 0;
      --size;

      byte memo = 0;
      if ((i / 8) + 1 < elements.length) {
        memo = (byte) ((1 & (elements[(i / 8) + 1])) << 7);
      }
      for (int index = (i / 8) + 1; index <= (size / 8); index++) {
        elements[index] = (byte) ((elements[index] & 0xff) >>> 1);
        if (index + 1 < elements.length && (elements[index + 1] & 1) == 1) {
          elements[index] |= 1 << (Byte.SIZE - 1);
        }
      }
      // 87654321 => <memo>8764321
      byte start = (byte) ((1 << ((i + 1) % 8)) - 1);
      byte end = (byte) ~start;
      elements[i / 8] = (byte) ((((start & 0xff) >>> 1) & elements[i / 8]) // 1234
          | (end & (elements[i / 8] >> 1)) // 876
          | memo);

      return result;
    }

    @Override
    public Boolean peek() {
      return (size < elements.length * Byte.SIZE) ? (elements[(size / 8)] & (1 << (size % 8))) > 0 : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      boolean tmp = this.get(index1);
      this.set(index1, this.get(index2));
      this.set(index2, tmp);
    }
  }

  public static class FloatArray extends GenericData.AbstractArray<Float> {
    private static final float[] EMPTY = new float[0];

    private float[] elements = EMPTY;

    public FloatArray(int capacity, Schema schema) {
      super(schema);
      if (!Schema.Type.FLOAT.equals(schema.getElementType().getType()))
        throw new AvroRuntimeException("Not a float array schema: " + schema);
      if (capacity != 0)
        elements = new float[capacity];
    }

    public FloatArray(Schema schema, Collection<Float> c) {
      super(schema);
      if (c != null) {
        elements = new float[c.size()];
        addAll(c);
      }
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public Float get(int i) {
      return this.getFloat(i);
    }

    /**
     * Direct primitive int access.
     * 
     * @param i : index.
     * @return value at index.
     */
    public float getFloat(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return elements[i];
    }

    @Override
    public void add(int location, Float o) {
      if (o == null) {
        return;
      }
      this.add(location, o.floatValue());
    }

    public void add(int location, float o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length) {
        // Increase size by 1.5x + 1
        final int newSize = size + (size >> 1) + 1;
        elements = Arrays.copyOf(elements, newSize);
      }
      System.arraycopy(elements, location, elements, location + 1, size - location);
      elements[location] = o;
      size++;
    }

    @Override
    public Float set(int i, Float o) {
      if (o == null) {
        return null;
      }
      return this.set(i, o.floatValue());
    }

    public float set(int i, float o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      float response = elements[i];
      elements[i] = o;
      return response;
    }

    @Override
    public Float remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      float result = elements[i];
      --size;
      System.arraycopy(elements, i + 1, elements, i, (size - i));
      return result;
    }

    @Override
    public Float peek() {
      return (size < elements.length) ? elements[size] : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      float tmp = this.get(index1);
      this.set(index1, this.get(index2));
      this.set(index2, tmp);
    }
  }

  public static class DoubleArray extends GenericData.AbstractArray<Double> {
    private static final double[] EMPTY = new double[0];

    private double[] elements = EMPTY;

    public DoubleArray(int capacity, Schema schema) {
      super(schema);
      if (!Schema.Type.DOUBLE.equals(schema.getElementType().getType()))
        throw new AvroRuntimeException("Not a double array schema: " + schema);
      if (capacity != 0)
        elements = new double[capacity];
    }

    public DoubleArray(Schema schema, Collection<Double> c) {
      super(schema);
      if (c != null) {
        elements = new double[c.size()];
        addAll(c);
      }
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public Double get(int i) {
      return this.getDouble(i);
    }

    /**
     * Direct primitive int access.
     * 
     * @param i : index.
     * @return value at index.
     */
    public double getDouble(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return elements[i];
    }

    @Override
    public void add(int location, Double o) {
      if (o == null) {
        return;
      }
      this.add(location, o.floatValue());
    }

    public void add(int location, double o) {
      if (location > size || location < 0) {
        throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
      }
      if (size == elements.length) {
        // Increase size by 1.5x + 1
        final int newSize = size + (size >> 1) + 1;
        elements = Arrays.copyOf(elements, newSize);
      }
      System.arraycopy(elements, location, elements, location + 1, size - location);
      elements[location] = o;
      size++;
    }

    @Override
    public Double set(int i, Double o) {
      if (o == null) {
        return null;
      }
      return this.set(i, o.floatValue());
    }

    public double set(int i, double o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      double response = elements[i];
      elements[i] = o;
      return response;
    }

    @Override
    public Double remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      double result = elements[i];
      --size;
      System.arraycopy(elements, i + 1, elements, i, (size - i));
      return result;
    }

    @Override
    public Double peek() {
      return (size < elements.length) ? elements[size] : null;
    }

    @Override
    protected void swap(final int index1, final int index2) {
      double tmp = this.get(index1);
      this.set(index1, this.get(index2));
      this.set(index2, tmp);
    }
  }

}
