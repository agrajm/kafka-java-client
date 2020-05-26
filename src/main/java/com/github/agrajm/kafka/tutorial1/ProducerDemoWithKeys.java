package com.github.agrajm.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        String topic = "mytopic";

        for(int i=0; i<10; i++) {
            String key = "key"+i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, "first_value"+i);

            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("*************************");
                    System.out.println("Offset: "+metadata.offset());
                    System.out.println("Topic: "+metadata.topic());
                    System.out.println("Partition: "+metadata.partition());
                    System.out.println("Timestamp: "+metadata.timestamp());
                }
            });
        }
        
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
