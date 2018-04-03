package com.hpeu.sparkStreaming.Producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Administrator on 2018/3/30/030.
 * This is a wrapper for KafkaProducer
 * This wrapper can send records to any KAFKA topic;
 * the user can specify to send records in sync or async mode;
 * note that:
 * The logic will automatically detect number of partitions for a topic;
 * the user prepared initial records should have topic, timestamp, key and value;
 * the key will be used to decide which partition the record should be appended to;
  */
public class NewKafkaProducer {

    private String mode;
    private Properties props;
    private KafkaProducer producer;

    public NewKafkaProducer(String mode) {
        this.mode = mode;
        props = prepareProducerProperties();
        producer = new KafkaProducer<String, Integer>(props);

    }

    /**
     * prepare producer properties.
     * @return producer properties.
     */
    private Properties prepareProducerProperties() {
        props = new Properties();
        try {
            InputStream propsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("producer.props");

            props.load(propsStream);

            if (!props.containsKey("bootstrap.servers")){
                System.out.println("You must specify boot strap servers!");
                System.exit(-1);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return props;
        }
    }

    /**
     * produce and send record
     * @param topic
     * @param timeStamp
     * @param key
     * @param value
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void produceAndSendRecord(String topic, Long timeStamp, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, timeStamp,key,value);
        send(record);
    }

    /**
     * send records
     * @param record
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void send(ProducerRecord<String, String> record) throws ExecutionException, InterruptedException {
        if (mode=="Async") {
            sendAsync(record);
        } else
            sendSync(record);
    }

    /**
     * Produce a record and wait for server to reply.
     * Throw an exception if something goes wrong
     * @param record
     */
    private void sendSync(ProducerRecord<String, String> record) throws ExecutionException, InterruptedException {
        RecordMetadata metaData = (RecordMetadata) producer.send(record).get();
        System.out.printf("successfully send record in sync mode,record:(key=%s, value =%s) " + "metadata:(topic=%s,partition=%d, timestamp=%d,offset=%d)\n", record.key(), record.value(), metaData.topic(), metaData.partition(), metaData.timestamp(), metaData.offset());

    }

    /**
     * Produce a record without waiting for server.
     * This includes a callback that will print an error if something goes wrong
     */
    private void sendAsync(ProducerRecord<String, String> record) {

        producer.send(record, new MyProducerCallback());

    }

    /**
     * The callback function used by Async mode send function
     */
    private class MyProducerCallback implements Callback {


        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.out.printf("error sending record in Async mode! " + "meta(topic=%s,partition=%d, timestamp=%d,offset=%d)\n", metadata.topic(), metadata.partition(), metadata.timestamp(), metadata.offset());
                exception.printStackTrace();
            } else {
                System.out.printf("successfully sending record in Async mode! " + "meta(topic=%s,partition=%d, timestamp=%d,offset=%d)\n", metadata.topic(), metadata.partition(), metadata.timestamp(), metadata.offset());
            }
        }
    }
}

