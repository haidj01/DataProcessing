package com.dkim.dataprocessing.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class CSVUtil {
    public static Dataset<Row> loadCsv(final SparkSession session, final String path,
                                       final StructType schema) {
        var dataFrameReader = session.read().format("csv");
        Map<String, String> optionMap = new HashMap<String, String>();
        optionMap.put("header","true");
        optionMap.put("escape","\"");
        if (schema!=null){
            dataFrameReader.schema(schema);
        }else {
            optionMap.put("inferSchema","true");
        }
        dataFrameReader.options(optionMap);

        return dataFrameReader.load(path);
    }
}
