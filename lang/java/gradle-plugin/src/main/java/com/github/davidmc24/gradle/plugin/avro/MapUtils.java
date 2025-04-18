/**
 * Copyright © 2015 Commerce Technologies, LLC.
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

import java.util.LinkedHashMap;
import java.util.Map;

class MapUtils {
    /**
     * Returns the map of all entries present in the first map but not present in the second map (by key).
     */
    static <K, V> Map<K, V> asymmetricDifference(Map<K, V> a, Map<K, V> b) {
        if (b == null || b.isEmpty()) {
            return a;
        }
        Map<K, V> result = new LinkedHashMap<>(a);
        result.keySet().removeAll(b.keySet());
        return result;
    }
}
