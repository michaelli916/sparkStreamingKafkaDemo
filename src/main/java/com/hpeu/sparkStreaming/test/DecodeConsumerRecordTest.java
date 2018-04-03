package com.hpeu.sparkStreaming.test;

import com.hpeu.sparkStreaming.beans.DeviceSignal;
import com.hpeu.sparkStreaming.utils.DecodeConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.Random;


/**
 * This test class is used to test the utility class
 * com.hpeu.sparkStreaming.utils.DecodeConsumerRecord.
 * Created by Administrator on 2018/4/1/001.
 */
public class DecodeConsumerRecordTest {
    public static void main(String[] argv) {
        Random random = new Random();
         /*
         String topic, int partition,long offset,long timestamp,
         TimestampType timestampType,long checksum, int serializedKeySize, int serializedValueSize,
         K key, V value)
          */
        for (int i = 0; i < 10; i++) {
            ConsumerRecord record = new ConsumerRecord("topic1", random.nextInt(5), random.nextLong(), System.currentTimeMillis(),
                    TimestampType.CREATE_TIME, random.nextLong(), random.nextInt(50), random.nextInt(50),
                    ("DT" + random.nextInt(5)).concat("#").concat("DID" + random.nextInt(5)), "SIGNAL" + random.nextInt(5));

            DeviceSignal signal = DecodeConsumerRecord.decodeConsumerRecord(record);
            System.out.println(signal);


        }


    }
}

