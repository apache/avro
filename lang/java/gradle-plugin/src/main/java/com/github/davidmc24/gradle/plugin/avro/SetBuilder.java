/**
 * Copyright © 2013-2015 Commerce Technologies, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.davidmc24.gradle.plugin.avro;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("UnusedReturnValue")
class SetBuilder<T> {
    private Set<T> set = new HashSet<T>();

    SetBuilder<T> add(T e) {
        set.add(e);
        return this;
    }

    final SetBuilder<T> addAll(T[] c) {
        Collections.addAll(set, c);
        return this;
    }

    SetBuilder<T> addAll(Collection<? extends T> c) {
        set.addAll(c);
        return this;
    }

    SetBuilder<T> remove(T e) {
        set.remove(e);
        return this;
    }

    Set<T> build() {
        return set;
    }
}
