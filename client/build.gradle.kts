import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.8.18"
    id("idea")
    id("signing")

    kotlin("jvm") version "1.6.10"
    id("com.diffplug.spotless") version "6.2.0"
}

group = "io.hstream"
version = "0.9.0"

repositories {
    mavenCentral()
}

java {
    withJavadocJar()
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    // grpc
    implementation("io.grpc:grpc-netty-shaded:1.45.0")
    implementation("io.grpc:grpc-protobuf:1.45.0")
    implementation("io.grpc:grpc-stub:1.45.0")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for java 9+

    implementation("com.google.protobuf:protobuf-java-util:3.17.3")

    compileOnly("org.slf4j:slf4j-api:1.7.30")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

    implementation("org.apache.commons:commons-lang3:3.12.0")

    api("com.google.guava:guava:30.1.1-jre")

    implementation("io.grpc:grpc-kotlin-stub:1.2.0")
    implementation("com.google.protobuf:protobuf-kotlin:3.17.3")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.5.2")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        outputs.upToDateWhen { false }
        showStandardStreams = true
    }
}

sourceSets {
    main {
        proto {
            exclude("api/*")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.19.2"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.45.0"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:1.2.1:jdk7@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "hstreamdb-java"
            from(components["java"])
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("hstreamdb-java")
                description.set("Java client for HStreamDB")
                url.set("https://hstream.io")
                licenses {
                    license {
                        name.set("The 3-Clause BSD License")
                        url.set("https://opensource.org/licenses/BSD-3-Clause")
                    }
                }
                developers {
                    developer {
                        id.set("daleiz")
                        name.set("Bin Wang")
                        email.set("wangbin@emqx.io")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/hstreamdb/hstreamdb-java.git")
                    developerConnection.set("scm:git:https://github.com/hstreamdb/hstreamdb-java.git")
                    url.set("https://github.com/hstreamdb/hstreamdb-java")
                }
            }
        }
    }
    repositories {
        maven {
            val releasesRepoUrl =
                "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
            // def releasesRepoUrl = layout.buildDirectory.dir('repos/releases')
            // def snapshotsRepoUrl = layout.buildDirectory.dir('repos/snapshots')
            url = uri(
                if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            )
            credentials {
                username =
                    if (project.hasProperty("ossrhUsername")) project.property("ossrhUsername") as String? else System.getenv(
                        "OSSRH_USERNAME"
                    )
                password =
                    if (project.hasProperty("ossrhPassword")) project.property("ossrhPassword") as String? else System.getenv(
                        "OSSRH_TOKEN"
                    )
            }
        }
    }
}

if (!project.hasProperty("disableSigning")) {
    signing {
        if (project.hasProperty("signing.keyId")) {
            sign(publishing.publications["mavenJava"])
        } else {
            val signingKey = System.getenv("OSSRH_GPG_SECRET_KEY")
            val signingPassword = System.getenv("OSSRH_GPG_PASSWORD")
            useInMemoryPgpKeys(signingKey, signingPassword)
            sign(publishing.publications["mavenJava"])
        }
    }
}

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    (options as StandardJavadocDocletOptions).links(
        "https://docs.oracle.com/en/java/javase/11/docs/api/",
        "https://javadoc.io/doc/com.google.guava/guava/latest/"
    )
    exclude("io/hstream/impl/**", "io/hstream/util/**")
}

spotless {
    java {
        googleJavaFormat()
    }

    kotlin {
        ktlint()
    }

    kotlinGradle {
        target("*.gradle.kts")
        ktlint()
    }
}
