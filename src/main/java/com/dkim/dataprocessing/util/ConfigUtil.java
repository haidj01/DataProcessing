package com.dkim.dataprocessing.util;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;


public class ConfigUtil {
    public static String getConfig(SparkSession session, String property) {
       return session.sparkContext().getConf().get(property);
    }

    public static String getConfig(SparkSession session, String property, String defaultValue) {
        return session.sparkContext().getConf().get(property, defaultValue);
    }

    public static String getGroupId(SparkSession session) {
        String groupId = getConfig(session, "spark.group.id", getUUID()) ;
        session.sparkContext().getConf().set("spark.group.id", groupId);
        return groupId;
    }

    public static String getDeltaTableFolderPath (SparkSession session) {
        return session.sparkContext().getConf().get("spark.delta.table.location");
    }

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }

    public static Dataset<Row> decorateDataSet(Dataset<Row> dataSet) {
        return dataSet.withColumn("group_id", lit(getGroupId(dataSet.sparkSession())))
            .withColumn("event_id", expr("uuid()"));
    }

    public static DeltaTable getDeltaTable(SparkSession session, String tableName) {
        try {
            return DeltaTable.forPath(session, getDeltaTableFolderPath(session) + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Delta Table was not found: " + tableName, e);
        }
    }

    public static Dataset<Row> readDeltaTable(SparkSession session, String tableName) {
        return session.read().format("delta").load(getDeltaTableFolderPath(session) + tableName);
    }

    public static String getCheckpointLocation (SparkSession session) {
        return session.sparkContext().getConf().get("spark.delta.table.checkpoint");
    }
}
