package com.dkim.dataprocessing.job;

import com.dkim.dataprocessing.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DataProcessJobManager {
    private static Map<String, DataProcess> JOB_MAP = new HashMap<>();

    static {
        JOB_MAP.put("BASE", new BaseProcessJob());
        JOB_MAP.put("COLLECT", new CollectProcessJob());
        JOB_MAP.put("SCHEMA", new DeltaSchema());
        JOB_MAP.put("EMP", new EmpJob());
        JOB_MAP.put("EMP_LOC", new EmpLocationJob());
        JOB_MAP.put("EMP_GLOBAL", new EmployeeGlobalPublishJob());
    }

    public static void executeJob(String[] args) throws TimeoutException, StreamingQueryException {
        if (args.length < 2){
            throw new IllegalArgumentException("need at least 2 ARGS");
        }
        boolean isLocal = "local=true".equalsIgnoreCase(args[0]);
        String jobName = args[1];
        SparkSession session = SparkSessionUtil.getSession(isLocal);
        DataProcess dataProcess  = JOB_MAP.get(jobName);
        if(dataProcess != null) {
            dataProcess.execute(session);
        }else {
            throw new RuntimeException("There is no job in given job name");
        }
    }
}

