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
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

  private static final String bootStrapServer = "127.0.0.1:9092";

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    System.out.println("Hello Producer Demo");

    //Create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //Set Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {

      String topic = "first_topic";
      String value = "Hello Consumer..!" + i + "!";
      String key = "id_" + i;

      log.info("keys: " + key);
      //      keys:id_0, Partition:1
      //      keys:id_8, Partition:1
      //
      //      keys:id_1, Partition:0
      //      keys:id_3, Partition:0
      //      keys:id_6, Partition:0
      //
      //      keys:id_2, Partition:2
      //      keys:id_4, Partition:2
      //      keys:id_5, Partition:2
      //      keys:id_7, Partition:2
      //      keys:id_9, Partition:2

      //Create Producer record
      ProducerRecord<String, String> record =
        new ProducerRecord<String, String>(topic, key, value);

      //send Data to kafka - To consumer. asynchronous.
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          //executes every time and record is successfully sent to an exception is thrown.
          if (e == null) {
            //if a record is not successfully sent.
            log.info("Received new metadata \n" +
              "Topic: "     + recordMetadata.topic() + "\n" +
              "Offset: "    + recordMetadata.offset() + "\n" +
              "Partition: " + recordMetadata.partition() + "\n" +
              "TimeStamp: " + recordMetadata.timestamp() + "\n"
            );
          } else {
            log.info("Error while producing");
          }
        }
      }).get(); //Blocks the .send() to make it synchronous. Never do in production.
    }

    //flush data
    producer.flush();

    //flush and closer producer.
    producer.close();
  }
}
