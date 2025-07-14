package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.util.CSVUtil;
import com.dkim.dataprocessing.util.ConfigUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class BaseProcessJob implements DataProcess {

    @Override
    public void execute(SparkSession session) {

        final String path = ConfigUtil.getConfig(session, "spark.base.path");
        Dataset<Row> ds = CSVUtil.loadCsv(session, path);
        ds = ds.withColumn("base_flag",
            when( col("base").equalTo(lit(1.0)), true)
                .otherwise(lit(false)));
        ds.show();
    }


}
