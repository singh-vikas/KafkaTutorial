package com.github.simplekafka.usecase.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  public static Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

  public static final String consumerKey = "skommoy7mLb6JnfNSFfmLHxwX";
  public static final String consumerSecret = "9A0MqA1nuuEu1WYhmhn56ia1Z3hdjziIWNSKe5QMHAS0Q5X49a";
  public static final String token = "923634506166177792-UlAHtJnQhxbJIVeyI853E06Mp7tewi5";
  public static final String secret = "s5YexKyHRvFIfc8InGmKeWrLoRSBX9f4ctwUKC3FVBRxK";
  //List<String> terms = Lists.newArrayList("bitcoin","usa","politics","sports");

  List<String> terms = Lists.newArrayList("kafka","stock","bitcoin");

  /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
  Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

  private TwitterProducer() {

  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {

    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    //Create a twitter Client and Attempts to establish a connection.
    Client client = createTwitterClient(msgQueue);
    client.connect();

    //Create a twitter producer
    KafkaProducer<String, String> producer = createTwitterProducer();

    //Add a shutdown hook.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Stopping application");
      log.info("Stopping client");
      client.stop();
      log.info("Stopping producer");
      producer.close();
      log.info("End of application");
    }));

    //Create a loop to send tweets to kafka .
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        log.info(msg);
        producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg),
          new Callback() {
            @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e != null) {
                log.info("Error while producing");

              }
            }
          });
      }
    }

    log.info("End of application");
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    ClientBuilder builder = new ClientBuilder()
      .name("Hosebird-Client-The_VKSingh")                              // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(authenticate())
      .endpoint(createHosebirdEndpoint())
      .processor(new StringDelimitedProcessor(msgQueue));
    //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events
    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  public Authentication authenticate() {

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    return hosebirdAuth;

  }

  public StatusesFilterEndpoint createHosebirdEndpoint() {
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    hosebirdEndpoint.trackTerms(terms);
    return hosebirdEndpoint;
  }

  private KafkaProducer<String, String> createTwitterProducer() {

    final String bootStrapServer = "127.0.0.1:9092";

    //Create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //Create Safe Producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    //These are not required if idempotent is set to true. Just to mention.
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka 2.0 > 1.0 else 1.

    //High Throughput settings
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32 KB Batch size

    //Set Producer
    return new KafkaProducer<String, String>(properties);
  }

}
