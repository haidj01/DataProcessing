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
                    .master("local[2]");
                setSparkConfig(builder, props);
            } else {
                builder.appName("Production");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return builder.getOrCreate();
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
