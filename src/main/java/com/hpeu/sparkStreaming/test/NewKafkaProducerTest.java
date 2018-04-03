package com.hpeu.sparkStreaming.test;

import com.hpeu.sparkStreaming.Producer.NewKafkaProducer;
import com.hpeu.sparkStreaming.beans.DeviceSignal;

import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * This test class is used to test com.hpeu.sparkStreaming.Producer.NewKafkaProducer
 * note that the key fields in ProducerRecord contains DevType and DevID of the
 * DeviceSignal records, and those two fields are concatenated using "#"
 * Created by Administrator on 2018/3/30/030.
 */
public class NewKafkaProducerTest {
    private static DeviceSignal prepareSignal(Random random){
        DeviceSignal signal = new DeviceSignal();
        signal.setDevType("DT"+random.nextInt(5));
        signal.setDevID("DID"+random.nextInt(100));
        signal.setTimestamp(System.currentTimeMillis());
        signal.setSignal("SIGNAL"+random.nextInt(5));
        return signal;
    }
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: NewKafkaProducerTest <topic> <mode>\n" +
                    "  <topic> is the kafka topic you want to create records for\n" +
                    "  <mode> is the mode in which you want to send records to kakfa, valid values are: sync,Async\n");
            System.exit(1);
        }

        String topic = args[0];
        String mode = args[1];

        NewKafkaProducer newKafkaProducer1 = new NewKafkaProducer(mode);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            try {
                final DeviceSignal signal = prepareSignal(random);
                newKafkaProducer1.produceAndSendRecord(topic,signal.getTimestamp(),signal.getDevType().concat("#").concat(signal.getDevID()),signal.getSignal());
                Thread.currentThread().sleep(1000);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
