package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.util.CSVUtil;
import com.dkim.dataprocessing.util.ConfigUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class CollectProcessJob implements DataProcess {
    @Override
    public void execute(SparkSession session) {
        String path = ConfigUtil.getConfig(session,"spark.collect.path");
        Dataset<Row> ds = CSVUtil.loadCsv(session,path,null);

        ds = ds.groupBy("id").agg(
            collect_list(
                to_json(
                    struct(col("sub_id"), col("value"))
                )
            ).alias(("to_json"))
        );
        ds.show(false);
    }
}
