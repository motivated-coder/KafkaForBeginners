package com.skd;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyProducer {
    private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);
    public static void main(String[] args) {

        // Create Producer properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);


        for(int i=0;i<10;i++){
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("Topic-B", "id_"+Integer.toString(i),
                "Hello World "+Integer.toString(i));

        kafkaProducer.send(producerRecord, (r,e) -> {
            if(e == null){
                logger.info("Topic : "+r.topic()+"\n"+
                        "Partition : "+r.partition()+"\n"+
                        "Offset : "+r.offset()+"\n"+
                        "Timestamp : "+r.timestamp());
            }
            else{
                logger.error("Error while sending",e);
            }
        });



    }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
