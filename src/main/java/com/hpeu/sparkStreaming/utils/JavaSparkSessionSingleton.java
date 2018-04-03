package com.hpeu.sparkStreaming.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/** Lazily instantiated singleton instance of SparkSession */
public class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .config("spark.sql.warehouse.dir", "/user/spark/warehouse")
                    .enableHiveSupport()
                    .getOrCreate();
        }
        return instance;
    }}