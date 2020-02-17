package com.github.vikasgautam18.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.ResourceBundle;

import static com.github.vikasgautam18.kafka.Constants.*;

public class ProducerExample {

    public static Logger logger = LoggerFactory.getLogger(ProducerExample.class);
    private static ResourceBundle producerProps = ResourceBundle.getBundle("producer");

    public static void main(String[] args) {
        // add all necessary kafka properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProps.getString(BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, producerProps.getString(ACKS));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerProps.getString(COMPRESSION_TYPE));

        // Producer Instance
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 1000; i++) {
            ProducerRecord<Long, String> record =
                    new ProducerRecord<>(producerProps.getString(OUTPUT_TOPIC), new Date().getTime(),
                            String.format("messagq %s", i));

            // Publish message
            producer.send(record, (recordMetadata, e) -> {
                if(e != null)
                    logger.error("Error producing record:: ", e);
                else
                    logger.info(String.format("Message was successfully produced to offset %s on partition %s at timestamp %s",
                            recordMetadata.offset(), recordMetadata.partition(), recordMetadata.timestamp()));
            });
        }
        producer.close();
    }
}
