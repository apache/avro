package com.commercehub.gradle.plugin.avro;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

class SetBuilder<T> {
    private Set<T> set = newHashSet();

    SetBuilder<T> add(T e) {
        set.add(e);
        return this;
    }

    @SafeVarargs
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

    static <T> Set<T> newHashSet() {
        return Sets.newHashSet();
    }

    @SafeVarargs
    static <T> Set<T> build(T... c) {
        return new SetBuilder<T>().addAll(c).build();
    }
}
