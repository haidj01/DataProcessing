package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.model.Model;
import com.dkim.dataprocessing.util.ConfigUtil;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class DeltaSchema implements DataProcess{
    @Override
    public void execute(SparkSession session) {
        for(Model.DeltaTables tables : Model.DeltaTables.values()) {
            String tableName = tables.getName();
            StructType newSchema = tables.getDecoratedSchema();

            if (tableExists(session, tableName)) {
                updateSchemaIfNeeded(session, tableName, newSchema);
            } else {
                createNewTable(session, tableName, newSchema);
            }
            DeltaTable deltaTable = ConfigUtil.getDeltaTable(session, tableName);
            deltaTable.toDF().printSchema();
        }
    }

    private  void createNewTable(SparkSession session, String tableName, StructType newSchema) {

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        StructField[] fields = newSchema.fields();

        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            sql.append(field.name()).append(" ").append(toSQLType(field.dataType()));
            if (i < fields.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(") USING DELTA LOCATION '").append(ConfigUtil.getDeltaTableFolderPath(session)).append(tableName)
            .append("' TBLPROPERTIES (delta.enableChangeDataFeed = true)");

        session.sql(sql.toString());

    }

    private void updateSchemaIfNeeded(SparkSession session, String tableName, StructType newSchema) {
        try {
            if(!tableExists(session, tableName)) {
                System.out.println(tableName + "is not found in delta table folder");
            }
            DeltaTable deltaTable = ConfigUtil.getDeltaTable(session, tableName);
            StructType currentSchema = deltaTable.toDF().schema();

            if (!isTableRegisteredInCatalog(session, tableName)) {
                forceRegisterTable(session, tableName);
                // 등록 후 잠시 대기 (Spark 메타데이터 갱신 시간)
                Thread.sleep(1000);

            }
            for (StructField newField : newSchema.fields()) {
                if (!hasField(currentSchema, newField.name())) {
                    addColumn(session, tableName, newField);
                }
            }

        }catch (Exception e) {
            System.err.println("Update Failed:" + tableName +"-" +e.getMessage());
        }

        // check current schema
        StructType currentSchema = ConfigUtil.getDeltaTable(session, tableName).toDF().schema();

        List<StructField> newFields = getNewFields(currentSchema, newSchema);
        for (StructField newField: newFields) {
            String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s; ALTER TABLE table_name SET TBLPROPERTIES (delta.enableChangeDataFeed=true);",
                    ConfigUtil.getDeltaTableFolderPath(session)+tableName, newField.name(), toSQLType(newField.dataType()));
                session.sql(alterSql);
        }
    }

    private List<StructField> getNewFields(StructType currentSchema, StructType newSchema) {
        List<StructField> newFields = new ArrayList<>();
        Set<String> currentFieldNames = Arrays.stream(currentSchema.fields())
            .map(StructField::name)
            .collect(Collectors.toSet());

        for (StructField field : newSchema.fields()) {
            if (!currentFieldNames.contains(field.name())) {
                newFields.add(field);
            }
        }
        return newFields;
    }

    private boolean hasField(StructType schema, String fieldName) {
        try {
            schema.fieldIndex(fieldName);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private void addColumn(SparkSession session, String tableName, StructField field) {
        try {
            String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                tableName, field.name(), toSQLType(field.dataType()));
            session.sql(alterSql);
            System.out.println("New Column Added: " + tableName + "." + field.name());
        } catch (Exception e) {
            System.err.println("Column addition is failed : " + field.name() + " - " + e.getMessage());
        }
    }


    private void ensureTableRegistered(SparkSession session, String tableName) {
        try {
            if (!session.catalog().tableExists(tableName)) {
                String tablePath = ConfigUtil.getDeltaTableFolderPath(session) + tableName;
                String registerSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s USING DELTA LOCATION '%s'",
                    tableName, tablePath
                );
                session.sql(registerSql);
                System.out.println("테이블 카탈로그에 등록됨: " + tableName);
            }
        } catch (Exception e) {
            System.err.println("테이블 등록 실패: " + tableName + " - " + e.getMessage());
        }
    }

    private boolean isTableRegisteredInCatalog(SparkSession session, String tableName) {
        try {
            return session.catalog().tableExists(tableName);
        } catch (Exception e) {
            return false;
        }
    }

    private void forceRegisterTable(SparkSession session, String tableName) {
        try {
            String tablePath = ConfigUtil.getDeltaTableFolderPath(session) + tableName;

            // 기존 테이블이 있다면 삭제 후 재등록
            try {
                session.sql("DROP TABLE IF EXISTS " + tableName);
            } catch (Exception e) {
                // 무시
            }

            // 새로 등록
            String registerSql = String.format(
                "CREATE TABLE %s USING DELTA LOCATION '%s'",
                tableName, tablePath
            );
            session.sql(registerSql);

            // 등록 확인
            session.sql("REFRESH TABLE " + tableName);
            System.out.println("테이블 강제 등록 완료: " + tableName);

        } catch (Exception e) {
            System.err.println("테이블 강제 등록 실패: " + tableName + " - " + e.getMessage());
            throw new RuntimeException("테이블 등록 실패", e);
        }
    }

    private String toSQLType(DataType dataType) {
        if (dataType.equals(DataTypes.StringType)) return "STRING";
        if (dataType.equals(DataTypes.IntegerType)) return "INT";
        if (dataType.equals(DataTypes.LongType)) return "BIGINT";
        if (dataType.equals(DataTypes.DoubleType)) return "DOUBLE";
        if (dataType.equals(DataTypes.BooleanType)) return "BOOLEAN";
        if (dataType.equals(DataTypes.TimestampType)) return "TIMESTAMP";
        if (dataType.equals(DataTypes.DateType)) return "DATE";

        // 복합 타입 처리
        if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            return "ARRAY<" + toSQLType(arrayType.elementType()) + ">";
        }
        if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return "MAP<" + toSQLType(mapType.keyType()) + "," + toSQLType(mapType.valueType()) + ">";
        }
        if (dataType instanceof StructType) {
            StructType structType = (StructType) dataType;
            StringBuilder sb = new StringBuilder("STRUCT<");
            StructField[] fields = structType.fields();
            for (int i = 0; i < fields.length; i++) {
                sb.append(fields[i].name()).append(":").append(toSQLType(fields[i].dataType()));
                if (i < fields.length - 1) sb.append(",");
            }
            sb.append(">");
            return sb.toString();
        }

        return "STRING";  // Default fallback
    }
}
