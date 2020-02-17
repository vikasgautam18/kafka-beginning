package com.github.vikasgautam18.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import org.junit.*;

import static java.util.Collections.singleton;

public class ProducerExampleTest {

    public static final String OUTPUT_TOPIC = "test_topic";
    public static final String SECOND_TOPIC = "test_topic_2";
    private static Consumer<Long, String> testConsumer;

    @ClassRule
    public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1,
            false, OUTPUT_TOPIC);

    @BeforeClass
    public static void setConsumer() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false",
                kafka.getEmbeddedKafka()));
        testConsumer = new DefaultKafkaConsumerFactory<>(configs, new LongDeserializer(), new StringDeserializer())
                .createConsumer();
        testConsumer.subscribe(singleton(SECOND_TOPIC));
        testConsumer.poll(Duration.ofMillis(1000));

    }

    @AfterClass
    public static void tearDown() {
        kafka.getEmbeddedKafka().destroy();
    }

    @Test
    public void testProducer() {

        Properties producderProperties = new Properties();
        producderProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getEmbeddedKafka().getBrokersAsString());
        producderProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producderProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producderProperties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        producderProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        KafkaProducer<Long, String> producer = new KafkaProducer<>(producderProperties);

        final Long timestamp = new Date().getTime();
        final ProducerRecord<Long, String> record =
                new ProducerRecord<>(OUTPUT_TOPIC, timestamp, "message");

        producer.send(record, (recordMetadata, e) -> {
            System.out.println("message successfully produced!");
            System.out.println("topic: " + recordMetadata.topic());
            System.out.println("offset: " + recordMetadata.offset());
            System.out.println("partition: " + recordMetadata.partition());
            System.out.println("timestamp: " + recordMetadata.timestamp());
        });
        producer.close();


        // consume the message produced above
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getEmbeddedKafka().getBrokersAsString());
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

        int count = 0;
        ConsumerRecords<Long, String> records = null;
        while(count == 0){
             records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<Long, String> rec : records) {
                System.out.println("message key: " + rec.key());
                System.out.println("message value: " +rec.value());
            }

             count++;
        }

        Assert.assertEquals(1, records.count());
        Assert.assertEquals("message", records.iterator().next().value());
        Assert.assertEquals(timestamp, records.iterator().next().key());
    }

    @Test
    public void testWithKafkaUtils() {

        final Long timestamp = new Date().getTime();
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(kafka.getEmbeddedKafka()));
        Producer<Long, String> producer = new DefaultKafkaProducerFactory<>(configs,
                new LongSerializer(), new StringSerializer()).createProducer();

        // add data to topic
        producer.send(new ProducerRecord<>(SECOND_TOPIC, timestamp, "hello"));
        producer.flush();

        // Assert results
        ConsumerRecord<Long, String> singleRecord = KafkaTestUtils.getSingleRecord(testConsumer, SECOND_TOPIC);
        Assert.assertNotNull(singleRecord);
        Assert.assertEquals(timestamp, singleRecord.key());
        Assert.assertEquals("hello", singleRecord.value());

    }
}