package com.dkim.dataprocessing.util;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkSessionUtil {

    public static SparkSession getSession(boolean isLocal) {
        Properties props = loadProperties();
        SparkSession.Builder builder = SparkSession.builder();
        try {
            if(isLocal) {
                builder.appName("local test")
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.driver.host", "localhost")
                    .config("spark.driver.port", "4040");
                setSparkConfig(builder, props);

            } else {
                builder.appName("Production");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
//            // 로컬 파일 시스템 설정 수정
//            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
//            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .getOrCreate();
    }
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = SparkSessionUtil.class
            .getClassLoader().getResourceAsStream("application.properties")){
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static void setSparkConfig(SparkSession.Builder builder, Properties pros) {
        pros.forEach((k, v) -> {
            String keyStr = k.toString();
            if(keyStr.startsWith("spark.")) {
                builder.config(keyStr, v.toString());
                }
            });
    }
}
