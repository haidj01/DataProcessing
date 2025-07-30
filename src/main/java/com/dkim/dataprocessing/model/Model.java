package com.dkim.dataprocessing.model;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedHashMap;
import java.util.Map;

public class Model {

   public static StructType EMP_SCHEMA = new StructType()
        .add("id", DataTypes.StringType)
        .add("name", DataTypes.StringType)
        .add("email", DataTypes.StringType)
        .add("age", DataTypes.IntegerType)
        .add("salary", DataTypes.DoubleType)
        .add("created_at", DataTypes.TimestampType)
        .add("is_active", DataTypes.BooleanType);

   public static StructType EMP_LOCATION_SCHEMA = new StructType()
       .add("id", DataTypes.StringType)
       .add("location", DataTypes.StringType)
       .add("address1", DataTypes.StringType)
       .add("address2", DataTypes.StringType)
       .add("city", DataTypes.StringType)
       .add("state", DataTypes.StringType)
       .add("zip", DataTypes.StringType);

   public static StructType EMP_GLOBAL_SCHEMA = mergeSchemas(EMP_SCHEMA, EMP_LOCATION_SCHEMA);

   public static StructType mergeSchemas(StructType... schemas) {
       Map<String, StructField> fieldMap = new LinkedHashMap<>();
       for (StructType schema : schemas) {
           for (StructField field : schema.fields()) {
               fieldMap.putIfAbsent(field.name(), field);
           }
       }

       StructType merged = new StructType();
       for (StructField field : fieldMap.values()) {
           merged = merged.add(field);
       }
       return merged;
   }

   public static StructType baseType = new StructType()
       .add("group_id", DataTypes.StringType)
       .add("event_id", DataTypes.StringType);

   public enum DeltaTables {
       EMP_TABLE("emp",EMP_SCHEMA),
       EMP_LOCATION_TABLE("emp_location", EMP_LOCATION_SCHEMA),
       EMP_GLOBAL_TABLE("emp_global", EMP_GLOBAL_SCHEMA);

       private final String name;
       private final StructType schema;
       private StructType decoratedSchema;

       DeltaTables(String name, StructType schema){
           this.name = name;
           this.schema = schema;
       }
       public String getName(){
           return this.name;
       }

       public StructType getDecoratedSchema() {
           return mergeSchemas(schema, baseType);
       }

       public StructType getSchema() {
           return this.schema;
       }
    }


}
