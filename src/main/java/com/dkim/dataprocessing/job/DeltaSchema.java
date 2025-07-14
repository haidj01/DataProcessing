package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.model.Model;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DeltaSchema implements DataProcess{
    @Override
    public void execute(SparkSession session) {
       for(Model.DeltaTables tables : Model.DeltaTables.values()){
           StringBuilder sql = new StringBuilder();
           sql.append("CREATE TABLE IF NOT EXISTS ").append(tables.getName()).append(" (");

           StructType schema = tables.getSchema();
           StructField[] fields = schema.fields();

           for (int i = 0; i < fields.length; i++) {
               StructField field = fields[i];
               sql.append(field.name()).append(" ").append(toSQLType(field.dataType()));
               if (i < fields.length - 1) {
                   sql.append(", ");
               }
           }
           sql.append(") USING DELTA LOCATION '").append("/Users/dkim/spark/delta_db/").append(tables.getName()).append("'");

           session.sql(sql.toString());
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
        // 필요한 경우 추가
        return "STRING";  // Default fallback
    }
}
