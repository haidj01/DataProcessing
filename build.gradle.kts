plugins {
    java
    application
}
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

group = "com.dkim.dataprocessing"
version = "0.1.0"


repositories {
    mavenCentral()
}



dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.5.1")
    implementation("org.apache.spark:spark-sql_2.12:3.5.1")
    implementation("org.apache.hadoop:hadoop-common:3.3.4")
    //implementation("io.delta:delta-core_2.12:3.1.0")
    implementation("io.delta:delta-spark_2.12:3.1.0")
    implementation("org.scala-lang:scala-library:2.12.18")
    testImplementation ("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly ("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}
