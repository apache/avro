plugins {
    id("idea")
    id("com.github.davidmc24.gradle.plugin.avro") version ("1.2.0")
//    id "com.github.davidmc24.gradle.plugin.avro" version "1.2.1-SNAPSHOT"
}

repositories {
    mavenCentral()
}

project.ext.set("avroVersion", "1.11.0")
dependencies {
    implementation("org.apache.avro:avro:${project.ext.get("avroVersion")}")
    implementation("org.apache.avro:avro-tools:${project.ext.get("avroVersion")}")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

avro {
    stringType.set("CharSequence")
    fieldVisibility.set("private")
    customConversion(org.apache.avro.Conversions.UUIDConversion::class.java)
}
