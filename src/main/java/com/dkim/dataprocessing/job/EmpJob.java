package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.model.Model;
import com.dkim.dataprocessing.util.CSVUtil;
import com.dkim.dataprocessing.util.ConfigUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EmpJob implements DataProcess {

    @Override
    public void execute(SparkSession session) {
        final String path = ConfigUtil.getConfig(session,"spark.emp.path");
        Dataset<Row> empDataSet = CSVUtil.loadCsv(session,path, Model.EMP_SCHEMA)
            .transform(ConfigUtil::decorateDataSet);
        Dataset<Row> cdc = mergeStagingDeltaTable(session, Model.DeltaTables.EMP_TABLE, empDataSet);
        mergeMasterDeltaTable(session,Model.DeltaTables.EMP_TABLE, cdc);
    }
}
