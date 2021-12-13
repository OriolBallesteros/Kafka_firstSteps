package com.daruma.kafkatwitter.producer;

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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

  private String consumerKey;
  private String consumerSecret;
  private String token;
  private String secret;

  private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    // Messages storage
    final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    // Create twitter client
    final Client client = createTwitterClient(msgQueue);
    // Try to establish connection
    client.connect();

    // Create Kafka producer
    final KafkaProducer<String, String> producer = createKafkaProducer();

    // shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Stopping application...");
      logger.info("Shutting down client from Twitter...");
      client.stop();
      logger.info("Closing producer...");
      producer.close();
      logger.info("Application stopped");
    }));

    // Get message from Twitter and send it to Kafka
    while (!client.isDone()) { // while Twitter connection is active

      // Get messages from Twitter
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }

      // Send to Kafka
      if (msg != null) {
        logger.info(msg);
        producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
          // Topic must be created before its usage.
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              logger.error("[ERROR OCURRED] - ", e);
            }
          }
        });
      }
    }
  }

  public KafkaProducer<String, String> createKafkaProducer() {
    final String bootstrapServer = "127.0.0.1:9092";

    // Create producer properties
    final Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Safe Producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    // High throughput producer (at the expense of a bit of latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

    final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    return producer;
  }

  public Client createTwitterClient(final BlockingQueue<String> msgQueue) {
    loadProperties();

    // -- Connection --
    final List<String> terms = Lists.newArrayList("bitcoin");
    final StatusesFilterEndpoint endPoint = new StatusesFilterEndpoint();
    endPoint.trackTerms(terms);
    final Hosts host = new HttpHosts(Constants.STREAM_HOST);
    final Authentication auth = new OAuth1(consumerKey, consumerSecret, "foo", secret);

    // -- Client --
    final ClientBuilder builder = new ClientBuilder()
        .name("myKafkaTwitterApplication")
        .hosts(host)
        .authentication(auth)
        .endpoint(endPoint)
        .processor(new StringDelimitedProcessor(msgQueue));
    final Client client = builder.build();
    return client;
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

    consumerKey = properties.getProperty("twitter.api.access.consumer.key");
    consumerSecret = properties.getProperty("twitter.api.access.consumer.secret");
    token = properties.getProperty("twitter.api.access.token");
    secret = properties.getProperty("twitter.api.access.secret");
  }
}
