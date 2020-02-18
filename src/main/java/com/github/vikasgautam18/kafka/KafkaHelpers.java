package com.github.vikasgautam18.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

import static com.github.vikasgautam18.kafka.Constants.BOOTSTRAP_SERVERS;

public class KafkaHelpers<K, V> {

    private static ResourceBundle consumerProps = ResourceBundle.getBundle("consumer");

    public List<ConsumerRecord<K, V>> getMessagesAtOffset(String topic, long offset, int numMessagesToConsume,
                                                     int partition, String keyDeserializer, String valueDeserializer){
        // consume the message produced above
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProps.getString(BOOTSTRAP_SERVERS));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);

        //assign partition
        TopicPartition partitionFrom = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(partitionFrom));

        //seek to offset
        consumer.seek(partitionFrom, offset);

        boolean keepConsuming = true;
        int messagesRead = 0;
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        while(keepConsuming){
            for (ConsumerRecord<K, V> record : consumer.poll(Duration.ofMillis(1000))) {
                records.add(record);
                messagesRead ++;
                if(messagesRead >= numMessagesToConsume)
                    keepConsuming = false;
            }
        }

        return records;
    }
}
