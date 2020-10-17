package com.github.simplekafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

  private static  final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

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


    for (int i=0; i<10 ; i++ ) {
      //Create Producer record
      ProducerRecord<String, String> record =
        new ProducerRecord<String, String>(
          "first_topic",
          "Hello Consumer..!!" + i +"!!");

      //send Data to kafka - To consumer. asynchronous.
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          //executes every time and record is successfully sent to an exception is thrown.
          if (e == null) {
            //if a record is not successfully sent.
            log.info("Received new metadata \n" +
              "Topic: " + recordMetadata.topic() + "\n" +
              "Offset: " + recordMetadata.offset() + "\n" +
              "Partition: " + recordMetadata.partition() + "\n" +
              "TimeStamp: " + recordMetadata.timestamp() + "\n"
            );
          } else {
            log.info("Error while producing \n" +
              "Topic: " + recordMetadata.topic() + "\n" +
              "Offset: " + recordMetadata.offset() + "\n" +
              "Partition: " + recordMetadata.partition() + "\n" +
              "TimeStamp: " + recordMetadata.timestamp() + "\n"
            );
          }
        }
      });
    }

    //flush data
    producer.flush();

    //close
    producer.close();
  }
}
