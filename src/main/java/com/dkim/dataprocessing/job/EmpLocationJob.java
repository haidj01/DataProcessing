package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.model.Model;
import com.dkim.dataprocessing.util.CSVUtil;
import com.dkim.dataprocessing.util.ConfigUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EmpLocationJob implements DataProcess {

    @Override
    public void execute(SparkSession session) {
        final String path = ConfigUtil.getConfig(session, "spark.emp.loc.path");
        Dataset<Row> empLocDs = CSVUtil.loadCsv(session, path, Model.EMP_LOCATION_SCHEMA)
            .transform(ConfigUtil::decorateDataSet);
        Dataset<Row> cdc = mergeStagingDeltaTable(session, Model.DeltaTables.EMP_LOCATION_TABLE, empLocDs);
        mergeMasterDeltaTable(session, Model.DeltaTables.EMP_LOCATION_TABLE, cdc);
    }
}
