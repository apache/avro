package com.commercehub.gradle.plugin.avro;

import org.apache.avro.Schema;
import org.gradle.api.GradleException;

import java.util.Set;
import java.util.TreeSet;

class TypeState {
    private final String name;
    private final Set<String> locations = new TreeSet<>();
    private Schema schema;

    TypeState(String name) {
        this.name = name;
    }

    void processTypeDefinition(String path, Schema schema) {
        locations.add(path);
        if (this.schema == null) {
            this.schema = schema;
        } else if (!this.schema.equals(schema)) {
            throw new GradleException(String.format("Found conflicting definition of type %s in %s", name, locations));
        } // Otherwise duplicate declaration of identical schema; nothing to do
    }

    String getName() {
        return name;
    }

    Schema getSchema() {
        return schema;
    }
}
