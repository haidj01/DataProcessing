package com.dkim.dataprocessing.model;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Model {

   public static StructType EMP_SCHEMA = new StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType)
        .add("email", DataTypes.StringType)
        .add("age", DataTypes.IntegerType)
        .add("salary", DataTypes.DoubleType)
       .add("created_at", DataTypes.TimestampType)
        .add("is_active", DataTypes.BooleanType);

   public enum DeltaTables {
       EMP("emp",EMP_SCHEMA);
       private final String name;
       private final StructType schema;

       DeltaTables(String name, StructType schema){
           this.name = name;
           this.schema = schema;
       }
       public String getName(){
           return this.name;
       }
       public StructType getSchema() {
           return this.schema;
       }
    }

}
