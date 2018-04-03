package com.hpeu.sparkStreaming.test;

import com.hpeu.sparkStreaming.beans.DeviceSignal;
import com.hpeu.sparkStreaming.utils.DecodeConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Random;


/**
 * This test class is used to check the records written out
 * by com.hpeu.sparkStreaming.Consumer.DirectKafkaConsumer, which can be
 * in json or parquet format
 * Created by Administrator on 2018/4/1/001.
 */
public class ReadSparkStreamingSavedFiles {
    public static void main(String[] argv) {
        if (argv.length < 2) {
            System.err.println("Usage: ReadSparkStreamingSavedFiles <path> <format>\n" +
                    "  <path> is the path of the file you want to read from \n" +
                    "  <format> is the format of the file you want to read from, currently only support json and parquet\n");
            System.exit(1);
        }
        String path = argv[0];
        String format = argv[1];

        SparkSession spark = SparkSession.builder().appName("ReadSparkStreamingSavedFiles").getOrCreate();

        if (format.equalsIgnoreCase("json")) {
            Dataset<Row> jsonDf = spark.read().json(path);
            jsonDf.printSchema();
            jsonDf.count();
            jsonDf.show();
        }
        if (format.equalsIgnoreCase("parquet")) {
            Dataset<Row> parquetDf = spark.read().parquet(path);
            parquetDf.printSchema();
            parquetDf.count();
            parquetDf.show();
        }

        spark.stop();
    }
}

