/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.avro.util;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements a combination of IdentityHashMap and ConcurrentMap using
 * WeakReference keys. Useful for caches that need to key off of a == comparison
 * instead of a .equals.
 * 
 * <b>
 * This class is not a general-purpose Map implementation! While
 * this class implements the Map interface, it intentionally violates
 * Map's general contract, which mandates the use of the equals method
 * when comparing objects. This class is designed for use only in the
 * rare cases wherein reference-equality semantics are required.
 * </b>
 */
public class WeakIdentityConcurrentMap<K, V> extends WeakIdentityMap<K, V> {

  public WeakIdentityConcurrentMap() {
    super(new ConcurrentHashMap<IdentityWeakReference<K>, V>());
  }

}
