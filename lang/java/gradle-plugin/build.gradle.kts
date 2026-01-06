/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    kotlin("jvm") version "2.2.10"
    `java-gradle-plugin`
    id("com.gradle.plugin-publish") version "2.0.0"
    id("com.vanniktech.maven.publish") version "0.35.0"
}

group = "eu.eventloopsoftware"
version = "0.0.6-SNAPSHOT"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    // for release use ${version}
    implementation("org.apache.avro:avro-compiler:1.12.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}


gradlePlugin {
    plugins {
        website = "https://avro.apache.org/"
        vcsUrl = "https://github.com/apache/avro.git"
        register("gradlePlugin") {
            id = "eu.eventloopsoftware.avro-gradle-plugin"
            displayName = "Avro Gradle Plugin"
            description = "Avro Gradle plugin for generating Java code"
            tags = listOf("avro", "kotlin", "java", "apache")
            implementationClass = "eu.eventloopsoftware.avro.gradle.plugin.GradlePlugin"
        }
    }
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(group.toString(), "avro-gradle-plugin", version.toString())
    pom {
        name = "Avro Gradle Plugin"
        description = "Generate Java code from Avro files"
        inceptionYear = "2025"
        url = "https://github.com/frevib/avro/"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "http://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }
        developers {
            developer {
                id = "frevib"
                name = "H. de Vries"
                url = "https://github.com/frevib/"
            }
        }
        scm {
            url = "https://github.com/frevib/avro/"
            connection = "scm:git:git://github.com/frevib/avro.git"
            developerConnection = "scm:git:ssh://git@github.com/frevib/avro.git"
        }
    }
}
