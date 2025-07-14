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
    implementation("org.apache.spark:spark-core_2.13:4.0.0")
    implementation("org.apache.spark:spark-sql_2.13:4.0.0")
    testImplementation ("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly ("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}
