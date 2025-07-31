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

import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.Schema;
import org.gradle.api.GradleException;

class TypeState {
    private final String name;
    private final Set<String> locations = new TreeSet<>();
    private Schema schema;

    TypeState(String name) {
        this.name = name;
    }

    void processTypeDefinition(String path, Schema schemaToProcess) {
        locations.add(path);
        if (this.schema == null) {
            this.schema = schemaToProcess;
        } else if (!this.schema.equals(schemaToProcess)) {
            throw new GradleException(String.format("Found conflicting definition of type %s in %s", name, locations));
        } // Otherwise duplicate declaration of identical schema; nothing to do
    }

    String getName() {
        return name;
    }

    Schema getSchema() {
        return schema;
    }

    boolean hasLocation(String location) {
        return locations.contains(location);
    }
}
