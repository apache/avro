package com.commercehub.gradle.plugin.avro;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class SetBuilder<T> {
    private Set<T> set = new HashSet<T>();

    SetBuilder<T> add(T e) {
        set.add(e);
        return this;
    }

    final SetBuilder<T> addAll(T... c) {
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

    static <T> Set<T> build(T... c) {
        return new SetBuilder<T>().addAll(c).build();
    }
}
