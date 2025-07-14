package com.dkim.dataprocessing.util;

import org.apache.spark.sql.SparkSession;

public class ConfigUtil {
    public static String getConfig(SparkSession session, String property) {
       return session.sparkContext().getConf().get(property);
    }
}
