package com.github.davidmc24.gradle.plugin.avro

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification
import spock.lang.Unroll

class SchemaResolverSpec extends Specification {
    private Project project
    private SchemaResolver resolver

    def setup() {
        project = ProjectBuilder.builder().build()
        resolver = new SchemaResolver(project.layout, project.logger)
    }

    @Unroll
    def "Can resolve records that use a separate record type (#resourceNames)"(List<String> resourceNames) {
        when:
        def files = resourceNames.collect { new File("src/test/resources/resolver/${it}") }
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == resourceNames.size()

        where:
        resourceNames << (
            ["SimpleRecord.avsc", "UseRecord.avsc"].permutations()
            + ["SimpleRecord.avsc", "UseRecordWithType.avsc"].permutations()
        )
    }

    @Unroll
    def "Can resolve records that use a separate enum type (#resourceNames)"(List<String> resourceNames) {
        when:
        def files = resourceNames.collect { new File("src/test/resources/resolver/${it}") }
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == resourceNames.size()

        where:
        resourceNames << (
            ["SimpleEnum.avsc", "UseEnum.avsc"].permutations()
            + ["SimpleEnum.avsc", "UseEnumWithType.avsc"].permutations()
        )
    }

    @Unroll
    def "Can resolve records that use a separate fixed type (#resourceNames)"(List<String> resourceNames) {
        when:
        def files = resourceNames.collect { new File("src/test/resources/resolver/${it}") }
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == resourceNames.size()

        where:
        resourceNames << (
            ["SimpleFixed.avsc", "UseFixed.avsc"].permutations()
            + ["SimpleFixed.avsc", "UseFixedWithType.avsc"].permutations()
        )
    }

    @Unroll
    def "Can resolve records that use a separate type in an array (#resourceNames)"(List<String> resourceNames) {
        when:
        def files = resourceNames.collect { new File("src/test/resources/resolver/${it}") }
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == resourceNames.size()

        where:
        resourceNames << (
            ["SimpleEnum.avsc", "SimpleRecord.avsc", "SimpleFixed.avsc", "UseArray.avsc"].permutations()
            + ["SimpleEnum.avsc", "SimpleRecord.avsc", "SimpleFixed.avsc", "UseArrayWithType.avsc"].permutations()
        )
    }

    @Unroll
    def "Can resolve records that use a separate type in an map value (#resourceNames)"(List<String> resourceNames) {
        when:
        def files = resourceNames.collect { new File("src/test/resources/resolver/${it}") }
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == resourceNames.size()

        where:
        resourceNames << (
            ["SimpleEnum.avsc", "SimpleRecord.avsc", "SimpleFixed.avsc", "UseMap.avsc"].permutations()
            + ["SimpleEnum.avsc", "SimpleRecord.avsc", "SimpleFixed.avsc", "UseMapWithType.avsc"].permutations()
        )
    }

    def "Duplicate record definition succeeds if definition identical"() {
        given:
        def resourceNames = ["Person.avsc", "Fish.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == files.size()
    }

    def "Duplicate enum definition succeeds if definition identical"() {
        given:
        def resourceNames = ["Person.avsc", "Cat.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == files.size()
    }

    def "Duplicate fixed definition succeeds if definition identical"() {
        given:
        def resourceNames = ["ContainsFixed1.avsc", "ContainsFixed2.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        def processingState = resolver.resolve(files)

        then:
        noExceptionThrown()
        processingState.failedFiles.empty
        processingState.processedTotal == files.size()
    }

    def "Duplicate record definition fails if definition differs"() {
        given:
        def resourceNames = ["Person.avsc", "Spider.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        resolver.resolve(files)

        then:
        def ex = thrown(GradleException)
        ex.message == "Found conflicting definition of type example.Person in [${files[0].path}, ${files[1].path}]"
    }

    def "Duplicate enum definition fails if definition differs"() {
        given:
        def resourceNames = ["Dog.avsc", "Person.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        resolver.resolve(files)

        then:
        def ex = thrown(GradleException)
        ex.message == "Found conflicting definition of type example.Gender in [${files[0].path}, ${files[1].path}]"
    }

    def "Duplicate fixed definition fails if definition differs"() {
        given:
        def resourceNames = ["ContainsFixed1.avsc", "ContainsFixed3.avsc"]
        def files = resourceNames.collect { new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/${it}") }

        when:
        resolver.resolve(files)

        then:
        def ex = thrown(GradleException)
        ex.message == "Found conflicting definition of type example.Picture in [${files[0].path}, ${files[1].path}]"
    }

    def "Duplicate record definition in single file fails with clear error"() {
        given:
        def file = new File("src/test/resources/com/github/davidmc24/gradle/plugin/avro/duplicate/duplicateInSingleFile.avsc")

        when:
        resolver.resolve([file])

        then:
        def ex = thrown(GradleException)
        ex.message == "Failed to resolve schema definition file ${file.path}; contains duplicate type definition example.avro.date"
    }
}
