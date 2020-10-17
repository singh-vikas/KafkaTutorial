package com.github.simplekafka.usecase.twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

  public static Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

  public static void main(String[] args) {
    RestHighLevelClient client = createElasticSearchClient();

    KafkaConsumer<String, String> consumer = createConsumer();

    //poll of new data.
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      Integer recordCount = records.count();
      log.info("Received " + recordCount + "records");

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> record : records) {
        String id = extractIdFromTweet(record.value());
        IndexRequest indexRequest =
          new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);
        bulkRequest.add(indexRequest);
      }

      if (recordCount > 0) {
        try {
          BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          e.printStackTrace();
        }

        log.info("Commit the offsets manually since: ENABLE_AUTO_COMMIT_CONFIG false");
        consumer.commitSync();
        log.info("offsets are committed now.");

        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static String extractIdFromTweet(String value) {
    return JsonParser.parseString(value).getAsJsonObject().get("id_str").getAsString();
  }

  public static RestHighLevelClient createElasticSearchClient() {

    String hostname = "kafka-course-8419957422.us-west-2.bonsaisearch.net";
    int port = 443;
    String username = "username";
    String password = "password";

    CredentialsProvider cp = new BasicCredentialsProvider();
    cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    // credentials provider help supply username and password
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
      new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(
      new HttpHost(hostname, 443, "https"))
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer() {

    final String bootStrapServer = "127.0.0.1:9092";
    final String groupId = "my-elastic-search-consumer";
    final String earliest = "earliest";
    final String topic = "twitter_tweets";

    //Create Consumer config.
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //Disable autocommit of offsets.
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //10 records at a time.

    //Create Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Arrays.asList(topic));
    return consumer;
  }
}
