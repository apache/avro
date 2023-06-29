package com.github.davidmc24.gradle.plugin.avro

import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

@Subject(Strings)
class StringsSpec extends Specification {
    @Unroll
    def "isEmpty(#str)"() {
        when:
        def actual = Strings.isEmpty(str)
        then:
        actual == expected
        where:
        str   | expected
        null  | true
        ""    | true
        " "   | false
        "abc" | false
    }

    @Unroll
    def "isNotEmpty(#str)"() {
        when:
        def actual = Strings.isNotEmpty(str)
        then:
        actual == expected
        where:
        str   | expected
        null  | false
        ""    | false
        " "   | true
        "abc" | true
    }

    @Unroll
    def "when not empty, requireNotEmpty returns argument (#str)"() {
        def message = "testMessage"
        expect:
        Strings.requireNotEmpty(str, message) == str
        where:
        str << [" ", "abc"]
    }

    @Unroll
    def "when empty, requireNotEmpty throws exception (#str)"() {
        def message = "testMessage"
        when:
        Strings.requireNotEmpty(str, message)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.message == message
        where:
        str << [null, ""]
    }
}
