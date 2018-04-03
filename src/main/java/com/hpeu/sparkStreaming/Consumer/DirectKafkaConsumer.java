package com.hpeu.sparkStreaming.Consumer;

import com.hpeu.sparkStreaming.beans.DeviceSignal;
import com.hpeu.sparkStreaming.utils.DecodeConsumerRecord;
import com.hpeu.sparkStreaming.utils.JavaSparkSessionSingleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.util.*;


/**
 * Created by Administrator on 2018/3/30/030.
 * let the user specify a threshold and use a broadcast variable for that
 */
public class DirectKafkaConsumer {


    public JavaInputDStream<ConsumerRecord<String, String>> setUpInputDStream(JavaStreamingContext jssc,String brokers, String consumergGroup,String autoOffsetReset,String topics){

        Set<String> topicSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", consumergGroup);
        kafkaParams.put("auto.offset.reset", autoOffsetReset);
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> consumerRecordDStream =
                KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, kafkaParams));
        return consumerRecordDStream;
    }

    /**
     * decode ConsumerRecord DStream into DeviceSignal DStream
     * @param consumerRecordDStream
     * @return DeviceSignal DStream
     */
    public JavaDStream<DeviceSignal> doDecode(JavaInputDStream<ConsumerRecord<String, String>>  consumerRecordDStream){
        JavaDStream<DeviceSignal> devSignalDStream = consumerRecordDStream.map(
                consumerRecord -> DecodeConsumerRecord.decodeConsumerRecord(consumerRecord));
        return devSignalDStream;
    }

    /**
     * do filter on DeviceSignal DStream using user provided signal
     * @param devSignalDStream
     * @param filteredSignal
     * @return filteredDevSignalDStream
     */
    public JavaDStream<DeviceSignal> doFilter(JavaDStream<DeviceSignal>  devSignalDStream,String filteredSignal){
        JavaDStream<DeviceSignal> filteredDevSignalDStream = devSignalDStream.filter
                (signal -> signal.getSignal().equals(filteredSignal));
        return filteredDevSignalDStream;
    }

    /**
     * This function will save the non empty RDDs from the DeviceSignal DStream into a
     * user specified directory;
     * note that sub directories are created under the user specified path, with year and
     * month as sub directory;
     * note that the final file name is named using the current timestamp.
     * @param devSignalDStream: the DeviceSignal DStream
     * @param path: the path under which files are written into
     */
    public void saveAsTextFiles(JavaDStream<DeviceSignal> devSignalDStream, String path){
        devSignalDStream.foreachRDD((rdd,time)->{
            if (!rdd.isEmpty()) {
                Calendar calendar1 = Calendar.getInstance();
                calendar1.setTimeInMillis(time.milliseconds());
                int year = calendar1.get(Calendar.YEAR);
                int month = (calendar1.get(Calendar.MONTH)+1);
                int day = calendar1.get(Calendar.DAY_OF_MONTH);

                rdd.saveAsTextFile(path+ File.separator+year+File.separator+
                        month+File.separator+time.milliseconds());}});
    }

    /**
     * This function will save the non empty RDDs from the DeviceSignal DStream into a
     * JSON file;
     * note that we are using APPEND save mode ;
     * note that we are partitioning on dev type and signal;
     * @param devSignalDStream
     * @param fileName
     */
    public void saveAsJsonFile(JavaDStream<DeviceSignal> devSignalDStream, String fileName){
        devSignalDStream.foreachRDD((rdd,time)->{
            if (!rdd.isEmpty()) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                Dataset<Row> filteredDevSignalDataFrame = spark.createDataFrame(rdd, DeviceSignal.class);
                filteredDevSignalDataFrame.write().mode(SaveMode.Append).partitionBy("devType","signal").json(fileName);
            }
        });

    }

    /**
     * This function will save the non empty RDDs from the DeviceSignal DStream into a
     * parquet file;
     * note that we are using APPEND save mode ;
     * note that we are partitioning on dev type and signal;
     * @param devSignalDStream
     * @param fileName
     */
    public void saveAsParquetFile(JavaDStream<DeviceSignal> devSignalDStream, String fileName){
        devSignalDStream.foreachRDD((rdd,time)->{
            if (!rdd.isEmpty()) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                Dataset<Row> filteredDevSignalDataFrame = spark.createDataFrame(rdd, DeviceSignal.class);
                filteredDevSignalDataFrame.write().mode(SaveMode.Append).partitionBy("devType","signal").parquet(fileName);
            }
        });

    }

    /**
     * This function will save the non empty RDDs into a user specified hive table;
     * note that we are saving data in APPEND mode, which means data are not overriden while appended
     * note that the hive table should be under a certain existing database
     * @param devSignalDStream: the dstream which will be saved;
     * @param tableName: the hive table we want to save data into.
     */
    public void saveAsHiveTable(JavaDStream<DeviceSignal> devSignalDStream, String tableName){
        devSignalDStream.foreachRDD((rdd,time)->{
            if (!rdd.isEmpty()) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                Dataset<Row> filteredDevSignalDataFrame = spark.createDataFrame(rdd, DeviceSignal.class);
                filteredDevSignalDataFrame.write().mode(SaveMode.Append).saveAsTable(tableName);
            }
        });
    }

}


