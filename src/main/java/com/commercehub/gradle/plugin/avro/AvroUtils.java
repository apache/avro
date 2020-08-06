package com.commercehub.gradle.plugin.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import static com.commercehub.gradle.plugin.avro.Constants.PROTOCOL_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.SCHEMA_EXTENSION;

class AvroUtils {
    /**
     * The namespace separator.
     */
    private static final String NAMESPACE_SEPARATOR = ".";

    /**
     * The extension separator.
     */
    private static final String EXTENSION_SEPARATOR = ".";

    /**
     * The Unix separator.
     */
    private static final String UNIX_SEPARATOR = "/";

    /**
     * Assembles a file path based on the namespace and name of the provided {@link Schema}.
     *
     * @param schema the schema for which to assemble a path
     * @return a file path
     */
    static String assemblePath(Schema schema) {
        return assemblePath(schema.getNamespace(), schema.getName(), SCHEMA_EXTENSION);
    }

    /**
     * Assembles a file path based on the namespace and name of the provided {@link Protocol}.
     *
     * @param protocol the protocol for which to assemble a path
     * @return a file path
     */
    static String assemblePath(Protocol protocol) {
        return assemblePath(protocol.getNamespace(), protocol.getName(), PROTOCOL_EXTENSION);
    }

    private static String assemblePath(String namespace, String name, String extension) {
        List<String> parts = new ArrayList<>();
        if (namespace != null && !namespace.isEmpty()) {
            parts.add(namespace.replaceAll(Pattern.quote(NAMESPACE_SEPARATOR), UNIX_SEPARATOR));
        }
        parts.add(name + EXTENSION_SEPARATOR + extension);
        return String.join(UNIX_SEPARATOR, parts);
    }
}
