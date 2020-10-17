package com.github.simplekafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

  public static Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

  private static final String bootStrapServer = "127.0.0.1:9092";
  //private static final String groupId = "my-intellij-consumer";
  private static final String earliest = "earliest";
  private static final String topic = "first_topic";

  public static void main(String[] args) {

    //Create Consumer config.
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
   // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);

    //Create Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    //Subscribe consumer to topic(s)
    //consumer.subscribe(Collections.singleton("first_topic"));
    // consumer.subscribe(Arrays.asList(topic));

    int partitionNumberToRead = 0; // The partition we want to read from.

    //Partition to read from - ASSIGN
    TopicPartition partitionToReadFrom = new TopicPartition(topic, partitionNumberToRead);
    //Assign topic partition to consumer.
    consumer.assign(Arrays.asList(partitionToReadFrom));

    long offSetToReadFrom = 15L;
    //Seek the partition
    consumer.seek(partitionToReadFrom, offSetToReadFrom);

    int numberOfMessagesRead = 0;
    int numberofMessagesToExit = 5;

    boolean continueReading = true;
    //poll of new data.
    while (continueReading) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesRead++;
        log.info("Key: " + record.key() + ", value: " + record.value() + "topic: " + record.topic());
        log.info("Partition: " + record.partition() + ", offset: " + record.offset());

        if (numberOfMessagesRead > numberofMessagesToExit) {
          continueReading = false; //exit While loop
          break; //exit for loop.
        }
      }
    }
  }
}
