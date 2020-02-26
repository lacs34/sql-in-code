import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	kotlin("jvm") version "1.3.61"
}

group = "com.losttemple"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
	implementation("mysql:mysql-connector-java:8.0.18")
	implementation("com.h2database:h2:1.4.200")
	implementation("com.github.salomonbrys.kotson:kotson:2.5.0")
	testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
	testImplementation("org.junit.jupiter:junit-jupiter-engine:5.6.0")
}

tasks.withType<Test> {
	useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "1.8"
	}
}
