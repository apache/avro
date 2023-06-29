package com.github.davidmc24.gradle.plugin.avro

import org.hamcrest.MatcherAssert
import spock.lang.Subject
import uk.co.datumedge.hamcrest.json.SameJSONAs

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

@Subject(ResolveAvroDependenciesTask)
class ResolveAvroDependenciesTaskFunctionalSpec extends FunctionalSpec {
    def "resolves dependencies"() {
        def srcDir = projectFolder("src/avro/normalized")

        given: "a build with the task declared"
        applyAvroBasePlugin()
        buildFile << """
        |tasks.register("resolveAvroDependencies", com.github.davidmc24.gradle.plugin.avro.ResolveAvroDependenciesTask) {
        |    source file("src/avro/normalized")
        |    outputDir = file("build/avro/resolved")
        |}
        |""".stripMargin()

        and: "some normalized schema files"
        copyResource("/examples/separate/Breed.avsc", srcDir)
        copyResource("/examples/separate/Cat.avsc", srcDir)

        when: "running the task"
        def result = run("resolveAvroDependencies")

        then: "the resolved schema files are generated"
        result.task(":resolveAvroDependencies").outcome == SUCCESS
        MatcherAssert.assertThat(
            projectFile("build/avro/resolved/example/Cat.avsc").text,
            SameJSONAs.sameJSONAs(getClass().getResourceAsStream("/examples/inline/Cat.avsc").text))
        MatcherAssert.assertThat(
            projectFile("build/avro/resolved/example/Breed.avsc").text,
            SameJSONAs.sameJSONAs(getClass().getResourceAsStream("/examples/separate/Breed.avsc").text))
    }
}
