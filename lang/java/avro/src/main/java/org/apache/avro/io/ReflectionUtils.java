/*
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
package org.apache.avro.io;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.function.Function;
import java.util.function.Supplier;

public class ReflectionUtils {

  private ReflectionUtils() {
    // static helper class, don't initiate
  }

  public static <D> Supplier<D> getConstructorAsSupplier(Class<D> clazz) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle constructorHandle = lookup.findConstructor(clazz, MethodType.methodType(void.class));

      CallSite site = LambdaMetafactory.metafactory(lookup, "get", MethodType.methodType(Supplier.class),
          constructorHandle.type().generic(), constructorHandle, constructorHandle.type());

      return (Supplier<D>) site.getTarget().invokeExact();
    } catch (Throwable t) {
      // if anything goes wrong, don't provide a Supplier
      return null;
    }
  }

  public static <V, R> Supplier<R> getOneArgConstructorAsSupplier(Class<R> clazz, Class<V> argumentClass, V argument) {
    Function<V, R> supplierFunction = getConstructorAsFunction(argumentClass, clazz);
    if (supplierFunction != null) {
      return () -> supplierFunction.apply(argument);
    } else {
      return null;
    }
  }

  public static <V, R> Function<V, R> getConstructorAsFunction(Class<V> parameterClass, Class<R> clazz) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle constructorHandle = lookup.findConstructor(clazz, MethodType.methodType(void.class, parameterClass));

      CallSite site = LambdaMetafactory.metafactory(lookup, "apply", MethodType.methodType(Function.class),
          constructorHandle.type().generic(), constructorHandle, constructorHandle.type());

      return (Function<V, R>) site.getTarget().invokeExact();
    } catch (Throwable t) {
      // if something goes wrong, do not provide a Function instance
      return null;
    }
  }

}
