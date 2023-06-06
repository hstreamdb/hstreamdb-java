import com.google.protobuf.gradle.id

plugins {
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.9.3"
    id("idea")
    id("signing")

    kotlin("jvm") version "1.6.10"
    id("com.diffplug.spotless") version "6.2.0"

    id("me.champeau.jmh") version "0.6.7"
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "io.hstream"
version = "0.16.0-SNAPSHOT"

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

val grpcVersion = "1.54.1"
val grpcKtVersion = "1.3.0"
val protobufVersion = "3.22.3"

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testImplementation("org.mockito:mockito-core:5.1.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    // grpc
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for java 9+

    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    compileOnly("org.slf4j:slf4j-api:2.0.6")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.19.0")

    implementation("org.apache.commons:commons-lang3:3.12.0")

    api("com.google.guava:guava:31.1-jre")

    implementation("io.grpc:grpc-kotlin-stub:$grpcKtVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")

    implementation("com.github.luben:zstd-jni:1.5.2-3")
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
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKtVersion:jdk8@jar"
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
            project.shadow.component(this)
            artifactId = "hstreamdb-java"
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
        // "https://javadoc.io/doc/com.google.guava/guava/latest/"
    )
    exclude("io/hstream/impl/**", "io/hstream/util/**")
}

val clientVersion = version
tasks.withType<Jar> {
    manifest {
        attributes["Implementation-Version"] = clientVersion
    }
}

spotless {
    java {
        target("client/src/*/java/**/*.java")
        googleJavaFormat()
    }

    kotlin {
        target("client/src/*/kotlin/**/*.kt")
        ktlint()
    }

    kotlinGradle {
        target("*.gradle.kts")
        ktlint()
    }
}

tasks {
    shadowJar {
        archiveClassifier.set("")
    }
}

jmh {
}
