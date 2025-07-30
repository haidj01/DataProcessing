package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.model.Model;
import com.dkim.dataprocessing.util.ConfigUtil;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public interface DataProcess {
        public void execute(SparkSession session) throws TimeoutException, StreamingQueryException;

        default Map<String, Column> getInsertMap(Model.DeltaTables deltaTables){
                Map<String, Column> resultMap = new HashMap<>();
                for(StructField f:  deltaTables.getDecoratedSchema().fields()) {
                        resultMap.put(f.name(),col("source."+ f.name()));
                }
                return  resultMap;
        }

        default Map<String, Column> getUpdateMap(Model.DeltaTables deltaTables){
                Map<String, Column> resultMap = new HashMap<>();
                for(StructField f:  deltaTables.getDecoratedSchema().fields()) {
                        if ("id".equalsIgnoreCase(f.name()))
                                continue;
                        resultMap.put(f.name(),col("source."+ f.name()) );
                }
                return  resultMap;
        }
        default String getMatchedCondition(Model.DeltaTables deltaTable) {
                StringBuilder builder = new StringBuilder();
                boolean needOr = false;
                for (String columnName : deltaTable.getSchema().fieldNames()) {
                        if("id".equalsIgnoreCase(columnName)){
                                continue;
                        }
                        if(needOr) {
                                builder.append(" OR ");
                        }
                        builder.append("(");
                        builder.append("( source."+columnName+" IS NOT NULL AND target."+columnName+" IS NULL )");
                        builder.append(" OR ( source."+columnName+" IS NULL AND target."+columnName+" IS NOT NULL )");
                        builder.append(" OR ( source."+columnName +" IS NOT NULL AND target." +columnName+" IS NOT NULL AND source." + columnName + "<> target."+columnName+ ")");
                        builder.append(")");
                        needOr = true;
                }
                return builder.toString();
        }

        default void mergeMasterDeltaTable(SparkSession session, Model.DeltaTables deltaTable, Dataset<Row> dataSet ) {
                DeltaTable.forPath(ConfigUtil.getConfig(session, "spark.delta.table.location") + "emp_global")
                    .as("target")
                    .merge(dataSet.as("source"), col("target.id").equalTo(col("source.id")))
                    .whenMatched(getMatchedCondition(deltaTable)).update(getUpdateMap(deltaTable))
                    .whenNotMatched().insert(getInsertMap(deltaTable))
                    .execute();
                appendCDC(session, dataSet);
        }


        default Dataset<Row> mergeStagingDeltaTable(SparkSession session, Model.DeltaTables deltaTable, Dataset<Row> dataSet){
                DeltaTable.forPath(ConfigUtil.getConfig(session, "spark.delta.table.location") + deltaTable.getName())
                    .as("target")
                    .merge(dataSet.as("source"), col("target.id").equalTo(col("source.id")))
                    .whenMatched(getMatchedCondition(deltaTable)).update(getUpdateMap(deltaTable))
                    .whenNotMatched().insert(getInsertMap(deltaTable))
                    .execute();

                DeltaTable resultTable = DeltaTable.forPath(session, ConfigUtil.getConfig(session, "spark.delta.table.location") + deltaTable.getName());
                Dataset<Row> history = resultTable.history(1);
                long lastVersion = history.collectAsList().get(0).getAs("version");

                return session.read().format("delta")
                    .option("readChangeData", "true")
                    .option("startingVersion", lastVersion)
                    .load(ConfigUtil.getConfig(session, "spark.delta.table.location") + deltaTable.getName())
                    .filter(col("_change_type").isin("update_postimage", "insert", "delete"));
        }

        default boolean tableExists(SparkSession session, String tableName) {
                try {
                        ConfigUtil.getDeltaTable(session, tableName);
                        return true;
                } catch (Exception e) {
                        return false;
                }
        }

        default void appendCDC(SparkSession session, Dataset<Row> cdc) {
                String tablePath = ConfigUtil.getConfig(session, "spark.delta.table.location") + "emp_cdc";

                try {
                        // 1. 테이블 상태 확인
                        boolean isFirstWrite = !tableExists(session, tablePath);

                        // 2. 데이터 쓰기
                        cdc.select("id", "group_id", "event_id")
                            .write()
                            .format("delta")
                            .mode(SaveMode.Append)
                            .option("mergeSchema", "true")  // 스키마 진화 허용
                            .save(tablePath);

                        // 3. 테이블 속성 설정 (처음 생성 시 또는 매번)
                        configureTableProperties(session, tablePath, isFirstWrite);

                        System.out.println("CDC 데이터 추가 완료: " + cdc.count() + "개 레코드");

                } catch (Exception e) {
                        System.err.println("CDC 데이터 추가 실패: " + e.getMessage());
                        throw new RuntimeException("CDC 처리 실패", e);
                }

        }

        private void configureTableProperties(SparkSession session, String tablePath, boolean isFirstWrite) {
                try {
                        if (isFirstWrite) {
                                // 처음 생성 시 모든 속성 설정
                                session.sql("ALTER TABLE delta.`" + tablePath + "` " +
                                    "SET TBLPROPERTIES (" +
                                    "delta.enableChangeDataFeed = true, " +
                                    "delta.deletedFileRetentionDuration = '7 days', " +
                                    "delta.logRetentionDuration = '30 days'" +
                                    ")");
                                System.out.println("테이블 속성 초기 설정 완료");
                        } else {
                                // 기존 테이블의 CDC 상태 확인
                                if (!isChangeDataFeedEnabled(session, tablePath)) {
                                        session.sql("ALTER TABLE delta.`" + tablePath + "` " +
                                            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)");
                                        System.out.println("Change Data Feed 활성화됨");
                                }
                        }
                } catch (Exception e) {
                        System.err.println("테이블 속성 설정 실패: " + e.getMessage());
                }
        }

        private boolean isChangeDataFeedEnabled(SparkSession session, String tablePath) {
                try {
                        Dataset<Row> properties = session.sql("DESCRIBE DETAIL delta.`" + tablePath + "`");
                        Row row = properties.first();
                        String props = row.getAs("properties").toString();
                        return props.contains("delta.enableChangeDataFeed=true");
                } catch (Exception e) {
                        return false;
                }
        }
}
