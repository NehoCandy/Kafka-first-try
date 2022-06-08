package io;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Lets start producing!");

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer itself
        //(for this example we're using string for both the key and the value)
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //create a producer record:
        ProducerRecord<String, String>  producerRecord = new ProducerRecord<>("demo_java", "hello_world!");


        //send data (asynchronous)
        producer.send(producerRecord);

        //flush and close  the producer (synchronous- to make sure the producer has send the data and only then got closed):
        producer.flush();
        producer.close();



    }
}
