package com.github.simplekafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

  public static void main(String[] args) {
    //Create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    //create a topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    //input topic
    KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
    KStream<String, String> filteredTopic = inputTopic.filter(
      (k, tweet) ->
        extractUserFollowers(tweet) > 1000
      //filter for tweets which has user of 100 followers.
    );
    filteredTopic.to("filtered_tweets");

    //build the topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

    kafkaStreams.start();

    //start our application

  }

  private static int extractUserFollowers(String value) {
    return JsonParser.parseString(value)
      .getAsJsonObject().get("user")
      .getAsJsonObject()
      .get("followers_count")
      .getAsInt();
  }
}
