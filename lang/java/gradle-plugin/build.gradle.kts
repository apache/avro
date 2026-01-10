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
}

group = "eu.eventloopsoftware"
version = "0.0.8"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    // TODO: for release use ${version}
    implementation("org.apache.avro:avro-compiler:1.12.1")
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin-api:2.3.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
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
            implementationClass = "eu.eventloopsoftware.avro.gradle.plugin.AvroGradlePlugin"
        }
    }
}
