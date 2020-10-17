package com.github.simplekafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

  private static Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

  private static final String bootStrapServer = "127.0.0.1:9092";
  private static final String groupId = "my-intellij-consumer";
  private static final String earliest = "earliest";
  private static final String topic = "first_topic";

  public static void main(String[] args) {
    new ConsumerDemoWithThreads().run();
  }

  private void run() {
    //Create Consumer config.
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);

    CountDownLatch latch = new CountDownLatch(1);

    log.info("Creating ConsumerThread");
    Runnable consumerThread = new ConsumerThread(properties, topic, latch);

    //Start the thread
    Thread consumerThead1 = new Thread(consumerThread);
    consumerThead1.start();

    //Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Caught shutdown hook");
      ((ConsumerThread) consumerThread).shutDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      log.info("Appication exited");

    }));

    try {
      latch.await();
    } catch (Exception e) {
      log.info("Application is interuppted");
    } finally {
      log.info("Application is closing");
    }

  }

  public class ConsumerThread implements Runnable {

    private KafkaConsumer<String, String> consumer;
    CountDownLatch latch;

    private ConsumerThread(Properties properties, String topic, CountDownLatch latch) {
      //Create consumer
      this.consumer = new KafkaConsumer<String, String>(properties);
      //Subscribe consumer to topic(s)
      //consumer.subscribe(Collections.singleton("first_topic")); // only one topic
      consumer.subscribe(Arrays.asList(topic));   //multiple topic.

      this.latch = latch;
    }

    @Override public void run() {

      try {
        //poll of new data.
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            log.info("Key: " + record.key() + ", value: " + record.value() + "topic: " + record.topic());
            log.info("Partition: " + record.partition() + ", offset: " + record.offset());
          }
        }
      } catch (WakeupException e) {
        log.info("Received shutdown signal.");
      } finally {
        consumer.close();
        //Main code to tell done with consumer.
        latch.countDown();
      }
    }

    //Shutdown cosmumer thread.
    public void shutDown() {
      consumer.wakeup();
    }
  }
}



