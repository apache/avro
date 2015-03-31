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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implements an IdentityMap with WeakReference keys.
 * Useful for caches that need to key off of a == comparison
 * instead of a .equals.
 * 
 * <b>
 * This class is not a general-purpose Map implementation! While
 * this class implements the Map interface, it intentionally violates
 * Map's general contract, which mandates the use of the equals method
 * when comparing objects. This class is designed for use only in the
 * rare cases wherein reference-equality semantics are required.
 *
 * Note that this implementation is not synchronized unless the underlying map
 * implementation is synchronized.
 * </b>
 */
class WeakIdentityMap<K, V> implements Map<K, V> {
  private final ReferenceQueue<K> queue = new ReferenceQueue<K>();
  private final Map<IdentityWeakReference<K>, V> backingStore;


  public WeakIdentityMap(Map<IdentityWeakReference<K>, V> backingStore) {
    this.backingStore = backingStore;
  }

  public void clear() {
    backingStore.clear();
    reap();
  }

  public boolean containsKey(Object key) {
    reap();
    return backingStore.containsKey(new IdentityWeakReference<K>(key, queue));
  }

  public boolean containsValue(Object value)  {
    reap();
    return backingStore.containsValue(value);
  }

  public Set<Entry<K, V>> entrySet() {
    reap();
    Set<Entry<K, V>> ret = new HashSet<Entry<K, V>>();
    for (Entry<IdentityWeakReference<K>, V> ref : backingStore.entrySet()) {
      final K key = ref.getKey().get();
      if (key == null) {
        continue;
      }

      final V value = ref.getValue();
      Entry<K, V> entry = new Entry<K, V>() {
        public K getKey() {
          return key;
        }
        public V getValue() {
          return value;
        }
        public V setValue(V value) {
          throw new UnsupportedOperationException();
        }
      };
      ret.add(entry);
    }
    return Collections.unmodifiableSet(ret);
  }

  public Set<K> keySet() {
    reap();
    Set<K> ret = new HashSet<K>();
    for (IdentityWeakReference<K> ref : backingStore.keySet()) {
      K key = ref.get();
      if (key != null) {
        ret.add(key);
      }
    }
    return Collections.unmodifiableSet(ret);
  }

  public boolean equals(Object o) {
    return backingStore.equals(((WeakIdentityMap)o).backingStore);
  }

  public V get(Object key) {
    reap();
    return backingStore.get(new IdentityWeakReference<K>(key, queue));
  }
  public V put(K key, V value) {
    reap();
    return backingStore.put(new IdentityWeakReference<K>(key, queue), value);
  }

  public int hashCode() {
    reap();
    return backingStore.hashCode();
  }
  public boolean isEmpty() {
    reap();
    return backingStore.isEmpty();
  }
  public void putAll(Map t) {
    throw new UnsupportedOperationException();
  }
  public V remove(Object key) {
    reap();
    return backingStore.remove(new IdentityWeakReference<K>(key, queue));
  }
  public int size() {
    reap();
    return backingStore.size();
  }
  public Collection<V> values() {
    reap();
    return backingStore.values();
  }

  private synchronized void reap() {
      Object zombie = queue.poll();

      while (zombie != null) {
        IdentityWeakReference victim = (IdentityWeakReference)zombie;
        backingStore.remove(victim);
        zombie = queue.poll();
      }
    }

  static class IdentityWeakReference<K> extends WeakReference<K> {
    private final int hash;
        
    @SuppressWarnings("unchecked")
    IdentityWeakReference(Object obj, ReferenceQueue<? super K> queue) {
      super((K)obj, queue);
      hash = System.identityHashCode(obj);
    }

    public int hashCode() {
      return hash;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      IdentityWeakReference ref = (IdentityWeakReference)o;
      if (this.get() == ref.get()) {
        return true;
      }
      return false;
    }
  }
}
