package com.github.davidmc24.gradle.plugin.avro

import org.apache.avro.Protocol
import org.apache.avro.Schema
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import static Constants.PROTOCOL_EXTENSION
import static Constants.SCHEMA_EXTENSION

@Subject(AvroUtils)
class AvroUtilsSpec extends Specification {
    private static final String EMPTY_STRING = ""
    private static final String SINGLE_LEVEL_NAMESPACE = "avro"
    private static final String MULTI_LEVEL_NAMESPACE = "org.example"
    private static final String MULTI_LEVEL_NAMESPACE_PATH = "org/example"
    private static final String SCHEMA_NAME = "SchemaName"
    private static final String PROTOCOL_NAME = "ProtocolName"

    @SuppressWarnings("ParameterName")
    @Unroll
    def "assemblePath rejects unnamed arguments (#arg)"(def arg, def _) {
        when:
        //noinspection GroovyAssignabilityCheck
        AvroUtils.assemblePath(arg)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.message == "Path cannot be assembled for nameless objects"
        where:
        arg                                    | _
        createSchema(null, null, true)         | _
        createSchema(null, EMPTY_STRING, true) | _
        createProtocol(null, null)             | _
        createProtocol(null, EMPTY_STRING)     | _
    }

    @Unroll
    def "assemblePath(#arg)"(def arg, String expectedPath) {
        when:
        //noinspection GroovyAssignabilityCheck
        def actualPath = AvroUtils.assemblePath(arg)
        then:
        actualPath == expectedPath
        where:
        arg                                                   | expectedPath
        createSchema(null, SCHEMA_NAME)                       | "${SCHEMA_NAME}.${SCHEMA_EXTENSION}"
        createSchema(EMPTY_STRING, SCHEMA_NAME)               | "${SCHEMA_NAME}.${SCHEMA_EXTENSION}"
        createSchema(SINGLE_LEVEL_NAMESPACE, SCHEMA_NAME)     | "${SINGLE_LEVEL_NAMESPACE}/${SCHEMA_NAME}.${SCHEMA_EXTENSION}"
        createSchema(MULTI_LEVEL_NAMESPACE, SCHEMA_NAME)      | "${MULTI_LEVEL_NAMESPACE_PATH}/${SCHEMA_NAME}.${SCHEMA_EXTENSION}"
        createProtocol(null, PROTOCOL_NAME)                   | "${PROTOCOL_NAME}.${PROTOCOL_EXTENSION}"
        createProtocol(EMPTY_STRING, PROTOCOL_NAME)           | "${PROTOCOL_NAME}.${PROTOCOL_EXTENSION}"
        createProtocol(SINGLE_LEVEL_NAMESPACE, PROTOCOL_NAME) | "${SINGLE_LEVEL_NAMESPACE}/${PROTOCOL_NAME}.${PROTOCOL_EXTENSION}"
        createProtocol(MULTI_LEVEL_NAMESPACE, PROTOCOL_NAME)  | "${MULTI_LEVEL_NAMESPACE_PATH}/${PROTOCOL_NAME}.${PROTOCOL_EXTENSION}"
    }

    Schema createSchema(String namespace, String name, boolean disableNameValidation = false) {
        if (disableNameValidation) {
            Schema.validateNames.set(false)
        }
        def schema = Schema.createRecord(name, null, namespace, false, Collections.emptyList())
        if (disableNameValidation) {
            Schema.validateNames.set(true)
        }
        return schema
    }

    Protocol createProtocol(String namespace, String name) {
        return new Protocol(name, null, namespace)
    }
}
