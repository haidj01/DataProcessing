package com.dkim.dataprocessing.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVUtil {
    public static Dataset<Row> loadCsv(final SparkSession session, final String path) {
       return session.read().format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .option("escape","\"")
            .load(path);
    }
}
