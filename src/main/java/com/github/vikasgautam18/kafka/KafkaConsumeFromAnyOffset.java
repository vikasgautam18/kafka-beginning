package com.github.vikasgautam18.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.ResourceBundle;

import static com.github.vikasgautam18.kafka.Constants.OUTPUT_TOPIC;

public class KafkaConsumeFromAnyOffset {
    private static ResourceBundle consumerProps = ResourceBundle.getBundle("consumer");

    public static void main(String[] args) {
        KafkaHelpers<Long, String> kafkaHelpers = new KafkaHelpers();

        List<ConsumerRecord<Long, String>> records =
                kafkaHelpers.getMessagesAtOffset(consumerProps.getString(OUTPUT_TOPIC), 12200,
                        22, 0, LongDeserializer.class.getName(),
                        StringDeserializer.class.getName());

        for (ConsumerRecord<Long, String> record:records) {
            System.out.println("message key: " + record.key());
            System.out.println("message value: " +record.value());
            System.out.println("=================================");
        }
    }
}
