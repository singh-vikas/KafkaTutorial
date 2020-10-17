package com.github.simplekafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {

  public static Logger log = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

  private static final String bootStrapServer = "127.0.0.1:9092";
  private static final String groupId = "my-intellij-2-consumer";
  private static final String earliest = "earliest";
  private static final String topic = "first_topic";

  public static void main(String[] args) {

    //Create Consumer config.
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);

    //Create Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    //Subscribe consumer to topic(s)
    //consumer.subscribe(Collections.singleton("first_topic")); // only one topic
    consumer.subscribe(Arrays.asList(topic));   //multiple topic.

    //poll of new data.
    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        log.info("Key: " + record.key() + ", value: " + record.value() + "topic: " + record.topic());
        log.info("Partition: " + record.partition() + ", offset: " + record.offset());
      }
    }
  }
}
