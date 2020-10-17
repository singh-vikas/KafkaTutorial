package com.github.simplekafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

  private static final String bootStrapServer= "127.0.0.1:9092";
  public static void main(String[] args) {
    System.out.println("Hello Producer Demo");

    //Create producer properties
    Properties properties=new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //Set Producer
    KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);


    //Create Producer record
    ProducerRecord<String, String> record=
      new ProducerRecord<String, String>("first_topic", "Hello Consumer..!!");

    //send Data to kafka - To consumer. asynchronous.
    producer.send(record);

   //flush data
    producer.flush();

    //closer
    producer.close();
  }
}
