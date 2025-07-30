package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.util.ConfigUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class EmployeeGlobalPublishJob implements DataProcess {

    @Override
    public void execute(SparkSession session) throws TimeoutException, StreamingQueryException {
        Dataset<Row> empCdc = session
            .readStream()
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .load(ConfigUtil.getDeltaTableFolderPath(session) + "emp_cdc");

        StreamingQuery query = empCdc
            .writeStream()
            .outputMode("update")
            .option("checkpointLocation",
                ConfigUtil.getCheckpointLocation(session) + "emp_cdc")
            .foreachBatch((batchDF, batchId) -> {
                System.out.println("====== BATCH ID: " + batchId + " ======");
                Dataset<Row> updatedEmpGlobal = batchDF.as("source")
                    .join(ConfigUtil.getDeltaTable(session, "emp_global").toDF().as("target"),
                    col("source.id").equalTo(col("target.id")));
                updatedEmpGlobal.show(false);

            })
            .trigger(Trigger.ProcessingTime("15 seconds"))
            .start();


        // 스트림 대기
        query.awaitTermination();


    }
}
