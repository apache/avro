package com.commercehub.gradle.plugin.avro;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapUtils {
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
