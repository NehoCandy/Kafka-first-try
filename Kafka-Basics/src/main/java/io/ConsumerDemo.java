package io;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Lets start consuming!");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-first-group";
        String autoOffset = "earliest"; // other options are 'none'(if no offset was provided, dont start)& 'latest'(read whats being sent now)
        String topic = "java3";

        //create consumer configurations
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);

        //create the consumer itself
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe our consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // wait for new data (poll)
        while(true){
            log.info("NOW POLLING!!");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                log.info("Key: " + record.key() + "value: " + record.value());
                log.info("Partition: " + record.partition() + "offset: " + record.offset());
            }
        }

    }
}
