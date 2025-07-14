package com.dkim.dataprocessing.job;

import org.apache.spark.sql.SparkSession;

public interface DataProcess {
        public void execute(SparkSession session);
}
