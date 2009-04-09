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

import java.util.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.lang.reflect.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.util.*;
import org.apache.avro.ipc.*;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericArray;

/** {@link Responder} for existing interfaces via Java reflection.*/
public class ReflectResponder extends Responder {
  private Object impl;
  protected String packageName;

  public ReflectResponder(Class iface, Object impl) {
    super(ReflectData.getProtocol(iface));
    this.impl = impl;
    this.packageName = getLocal().getNamespace()+"."+getLocal().getName()+"$";
  }

  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new ReflectDatumWriter(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new ReflectDatumReader(schema, packageName);
  }

  /** Reads a request message. */
  public Object readRequest(Schema schema, ValueReader in) throws IOException {
    Map<String,Schema> params = schema.getFields();
    Object[] args = new Object[params.size()];
    int i = 0;
    for (Map.Entry<String,Schema> param : params.entrySet())
      args[i++] = getDatumReader(param.getValue()).read(null, in);
    return args;
  }

  /** Writes a response message. */
  public void writeResponse(Schema schema, Object response, ValueWriter out)
    throws IOException {
    getDatumWriter(schema).write(response, out);
  }

  /** Writes an error message. */
  public void writeError(Schema schema, AvroRemoteException error,
                         ValueWriter out) throws IOException {
    getDatumWriter(schema).write(error, out);
  }

  public Object respond(Message message, Object request)
    throws AvroRemoteException {
    Map<String,Schema> params = message.getRequest().getFields();
    Class[] paramTypes = new Class[params.size()];
    int i = 0;
    try {
      for (Map.Entry<String,Schema> param : params.entrySet())
        paramTypes[i++] = paramType(param.getValue());
      Method method = impl.getClass().getMethod(message.getName(), paramTypes);
      return method.invoke(impl, (Object[])request);
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      if (target instanceof AvroRemoteException)
        throw (AvroRemoteException)target;
      else throw new AvroRuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new AvroRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private Class paramType(Schema schema) throws ClassNotFoundException {
    switch (schema.getType()) {
    case RECORD:  return Class.forName(packageName+schema.getName()); 
    case ARRAY:   return GenericArray.class;
    case MAP:     return Map.class;
    case UNION:   return Object.class;
    case STRING:  return Utf8.class;
    case BYTES:   return ByteBuffer.class;
    case INT:     return Integer.TYPE;
    case LONG:    return Long.TYPE;
    case FLOAT:   return Float.TYPE;
    case DOUBLE:  return Double.TYPE;
    case BOOLEAN: return Boolean.TYPE;
    case NULL:    return Void.TYPE;
    default: throw new AvroRuntimeException("Unknown type: "+schema);
    }

  }


}
