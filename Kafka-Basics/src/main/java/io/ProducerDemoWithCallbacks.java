package io;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());
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


//  NOTE: BECAUSE WE ARE SENDING DATA IN A LOOP- THEY ARE ALL BEING SENT TO THE SAME PARTITION DUE TO THE 'STICKY PARTITIONER'! MEANING KAFKA 'SEES' WE'RE SENDING A LOT OF DATA REALLY QUICKLY SO IT BATCHES IT TOGETHER AND SENDS IT ALL AT ONCE (INSTEAD OF IN ROUND-ROBIN)

        for(int i = 0; i<20; i++){
            //create a producer record:
            ProducerRecord<String, String>  producerRecord = new ProducerRecord<>("java2", "hello_world!" + i);
            //send data (asynchronous)
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes everytime a record was successfully sent or an exception was thrown
                    if(exception == null){
                        //no exception => successful send
                        log.info(("Recieved new metadata/ \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() +"\n"+
                                "Offset: " + metadata.offset() + "\n"+
                                "TimeStamp: " + metadata.timestamp()));
                    }
                    else{
                        log.error("Error while producing. exited with exception: "+exception);
                    }
                }
            });
            try{
                Thread.sleep((1000));
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }


        //flush and close  the producer (synchronous- to make sure the producer has send the data and only then got closed):
        producer.flush();
        producer.close();



    }
}
