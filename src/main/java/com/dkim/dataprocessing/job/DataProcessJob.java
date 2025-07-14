package com.dkim.dataprocessing.job;

public class DataProcessJob {
    public static void main (String[] args) {
        try {
            DataProcessJobManager.executeJob(args);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
