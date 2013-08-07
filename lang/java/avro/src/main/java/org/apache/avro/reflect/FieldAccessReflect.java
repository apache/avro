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
import java.lang.reflect.Field;

class FieldAccessReflect extends FieldAccess {

  @Override
  protected FieldAccessor getAccessor(Field field) {
    return new ReflectionBasedAccessor(field);
  }

  private final class ReflectionBasedAccessor extends FieldAccessor {
    private final Field field;
    private boolean isStringable;
    private boolean isCustomEncoded;

    public ReflectionBasedAccessor(Field field) {
      this.field = field;
      this.field.setAccessible(true);
      isStringable = field.isAnnotationPresent(Stringable.class);
      isCustomEncoded = field.isAnnotationPresent(AvroEncode.class); 
    }

    @Override
    public String toString() {
      return field.getName();
    }

    @Override
    public Object get(Object object) throws IllegalAccessException {
      return field.get(object);
    }

    @Override
    public void set(Object object, Object value) throws IllegalAccessException,
        IOException {
      field.set(object, value);
    }
    
    @Override
    protected Field getField() {
      return field;
    }
    
    @Override
    protected boolean isStringable() {
      return isStringable;
    }
    
    @Override
    protected boolean isCustomEncoded() {
      return isCustomEncoded;
    }

  }
}
