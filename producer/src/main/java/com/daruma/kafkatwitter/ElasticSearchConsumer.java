package com.daruma.kafkatwitter;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticSearchConsumer {

  private String hostname;
  private String user;
  private String pwd;

  public static void main(String[] args) throws IOException {
    new ElasticSearchConsumer().run();
  }


  public void run() throws IOException {
    final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    // Get ElasticSearch client
    final RestHighLevelClient client = createElasticSearchClient();
    // Get Kafka consumer
    final KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    while (true) {
      // Retrieve data from Kafka
      final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      final BulkRequest bulkRequest = new BulkRequest();

      // For each data set, save it on ElasticSearch
      for (ConsumerRecord<String, String> record : records) {
        // 2 strategies id generation --> consumer idempotent
        // Kafka generic Id
        // final String id = record.topic() + "_" + record.partition() + "_" + record.offset();
        // Twitter feed specific Id
        final String id = extractIdFromTweet(record.value());

        final IndexRequest indexRequest = new IndexRequest(
            "twitter",  // Requires PREVIOUSLY created index.
            "tweets",
            id) // Using an id makes our consumer idempotent
            .source(record.value(), XContentType.JSON);

        bulkRequest.add(indexRequest);
      }
      final BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
      // Commit offsets
      consumer.commitSync();
    }

    // client.close();
  }

  public KafkaConsumer<String, String> createConsumer(final String topic) {
    final String bootstrapServer = "127.0.0.1:9092";
    final String groupId = "kafka-twitter-elasticsearch";
    final String offsetConfig = "earliest";

    // Consumer Properties
    final Properties properties = new Properties();
    // Basic config
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    // Deserializer MUST be coincident with Producer serializer.
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    // Consumer config itself
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
    // Disable auto-commit of offsets
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


    // Create consumer itself
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Arrays.asList(topic));
    return consumer;
  }

  public RestHighLevelClient createElasticSearchClient() {
    loadProperties();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pwd));

    final RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(new HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(
              final HttpAsyncClientBuilder httpAsyncClientBuilder) {
            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
        });

    return new RestHighLevelClient(builder);
  }

  private String extractIdFromTweet(final String tweetJson) {
    return JsonParser.parseString(tweetJson)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
  }

  private void loadProperties() {
    final Properties properties = new Properties();
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    final InputStream stream = loader.getResourceAsStream("config.properties");

    try {
      properties.load(stream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    hostname = properties.getProperty("elasticsearch.client.hostname");
    user = properties.getProperty("elasticsearch.client.user");
    pwd = properties.getProperty("elasticsearch.client.pwd");
  }
}
