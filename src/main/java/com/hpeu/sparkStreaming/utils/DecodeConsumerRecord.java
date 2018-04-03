package com.hpeu.sparkStreaming.utils;

import com.hpeu.sparkStreaming.beans.DeviceSignal;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

/**
 * This utility class decodes Kafka ConsumerRecord into DeviceSignal records
 * note that the key fields in ConsumerRecord contains DevType and DevID of the
 * DeviceSignal records, and those two fields are split using "#"
 *
 * Created by Administrator on 2018/4/1/001.
 */
public class DecodeConsumerRecord {
    private static DeviceSignal signal;
    private static final Pattern p = Pattern.compile("#");

    public static DeviceSignal decodeConsumerRecord(ConsumerRecord record){
        signal = new DeviceSignal();
        signal.setDevType(p.split(record.key().toString())[0]);
        signal.setDevID(p.split(record.key().toString())[1]);
        signal.setSignal(record.value().toString());
        signal.setTimestamp(record.timestamp());
        return signal;

    }

}
