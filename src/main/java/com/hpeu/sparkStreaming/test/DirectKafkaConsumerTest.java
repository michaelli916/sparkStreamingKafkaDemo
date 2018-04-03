package com.hpeu.sparkStreaming.test;

import com.hpeu.sparkStreaming.Consumer.DirectKafkaConsumer;
import com.hpeu.sparkStreaming.beans.DeviceSignal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by Administrator on 2018/4/1/001.
 * The test class to test com.hpeu.sparkStreaming.Consumer.DirectKafkaConsumer
 */
public class DirectKafkaConsumerTest {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: DirectKafkaConsumer <brokers> <topics> <consumerGroup> <autoOffsetReset> <signal>\n" +
                    "  <brokers> is a list of bootstrap brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n" +
                    "  <consumerGroup> is the consumer group your group belongs to \n" +
                    "  <autoOffsetReset> should be one of 'earliest','latest' or 'none'\n" +
                    "  <signal> is the signal you want to filter on\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];
        String consumerGroup = args[2];
        String autoOffsetReset= args[3];
        String filteredSignal = args[4];

        SparkConf sc = new SparkConf().setAppName("DirectKafkaConsumer");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));

        DirectKafkaConsumer directKafkaConsumer = new DirectKafkaConsumer();

        JavaInputDStream<ConsumerRecord<String, String>> consumerRecordJavaInputDStream = directKafkaConsumer.setUpInputDStream(jssc,brokers,consumerGroup,autoOffsetReset,topics);

        JavaDStream<DeviceSignal> deviceSignalJavaDStream = directKafkaConsumer.doDecode(consumerRecordJavaInputDStream);

        JavaDStream<DeviceSignal> filteredDeviceSignalJavaDStream = directKafkaConsumer.doFilter(deviceSignalJavaDStream, filteredSignal);
/*
        //* save the non empty RDDs from the DeviceSignal DStream into text files
        //*sub directories are created using current year and month, file name is current timestamp
        directKafkaConsumer.saveAsTextFiles(deviceSignalJavaDStream,"deviceSignal");
        //* save the non empty RDDs from the DeviceSignal DStream into partitioned JSON files
        directKafkaConsumer.saveAsJsonFile(deviceSignalJavaDStream,"deviceSignal.json");
        //* save the non empty RDDs from the DeviceSignal DStream into partitioned parquet files
        directKafkaConsumer.saveAsParquetFile(deviceSignalJavaDStream,"deviceSignal.parquet");

        //* save the filtered non empty RDDs from the DeviceSignal DStream into text files
        //*sub directories are created using current year and month, file name is current timestamp
        directKafkaConsumer.saveAsTextFiles(filteredDeviceSignalJavaDStream,"filteredDeviceSignal");
        //* save the filtered non empty RDDs from the DeviceSignal DStream into partitioned JSON files
        directKafkaConsumer.saveAsJsonFile(filteredDeviceSignalJavaDStream,"filteredDeviceSignal.json");
        //* save the filtered non empty RDDs from the DeviceSignal DStream into partitioned parquet files
        directKafkaConsumer.saveAsParquetFile(filteredDeviceSignalJavaDStream,"filteredDeviceSignal.parquet");
*/
        directKafkaConsumer.saveAsHiveTable(deviceSignalJavaDStream,"spark.deviceSignal");
        directKafkaConsumer.saveAsHiveTable(filteredDeviceSignalJavaDStream,"spark.filteredDeviceSignal");
        jssc.start();
        jssc.awaitTermination();

    }
}

