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

import org.apache.avro.AvroRuntimeException;

/**
 * A few utility methods for using @link{java.misc.Unsafe}, mostly for private
 * use.
 * 
 * Use of Unsafe on Android is forbidden, as Android provides only a very
 * limited functionality for this class compared to the JDK version.
 * 
 */

class ReflectionUtil {

  private ReflectionUtil() {
  }

  private static final FieldAccess FIELD_ACCESS;
  static {
    // load only one implementation of FieldAccess
    // so it is monomorphic and the JIT can inline
    FieldAccess access = null;
    try {
      FieldAccess unsafeAccess = load(
          "org.apache.avro.reflect.FieldAccessUnsafe", FieldAccess.class);
      if (validate(unsafeAccess)) {
        access = unsafeAccess;
      }
    } catch (Throwable ignored) {
    }
    if (access == null) {
      try {
        FieldAccess reflectAccess = load(
            "org.apache.avro.reflect.FieldAccessReflect", FieldAccess.class);
        if (validate(reflectAccess)) {
          access = reflectAccess;
        }
      } catch (Throwable oops) {
        throw new AvroRuntimeException(
            "Unable to load a functional FieldAccess class!");
      }
    }
    FIELD_ACCESS = access;
  }

  private static <T> T load(String name, Class<T> type) throws Exception {
    return ReflectionUtil.class.getClassLoader().loadClass(name)
        .asSubclass(type).newInstance();
  }

  public static FieldAccess getFieldAccess() {
    return FIELD_ACCESS;
  }

  private static boolean validate(FieldAccess access) throws Exception {
    return new AccessorTestClass().validate(access);
  }

  private static final class AccessorTestClass {
    boolean b = true;
    byte by = 0xf;
    char c = 'c';
    short s = 123;
    int i = 999;
    long l = 12345L;
    float f = 2.2f;
    double d = 4.4d;
    Object o = "foo";
    Integer i2 = 555;

    private boolean validate(FieldAccess access) throws Exception {
      boolean valid = true;
      valid &= validField(access, "b", b, false);
      valid &= validField(access, "by", by, (byte) 0xaf);
      valid &= validField(access, "c", c, 'C');
      valid &= validField(access, "s", s, (short) 321);
      valid &= validField(access, "i", i, 111);
      valid &= validField(access, "l", l, 54321L);
      valid &= validField(access, "f", f, 0.2f);
      valid &= validField(access, "d", d, 0.4d);
      valid &= validField(access, "o", o, new Object());
      valid &= validField(access, "i2", i2, -555);
      return valid;
    }

    private boolean validField(FieldAccess access, String name,
        Object original, Object toSet) throws Exception {
      FieldAccessor a;
      boolean valid = true;
      a = accessor(access, name);
      valid &= original.equals(a.get(this));
      a.set(this, toSet);
      valid &= !original.equals(a.get(this));
      return valid;
    }

    private FieldAccessor accessor(FieldAccess access, String name)
        throws Exception {
      return access.getAccessor(this.getClass().getDeclaredField(name));
    }
  }

}
