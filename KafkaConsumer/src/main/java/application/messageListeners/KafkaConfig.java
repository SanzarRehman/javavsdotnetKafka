package application.messageListeners;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@ComponentScan(basePackages = "application")
@EnableKafka
public class KafkaConfig {

  @Bean
  public ConsumerFactory<Void, byte[]> consumerFactory() {
    Map<String, Object> consumerConfigProperties = new HashMap<>();
    consumerConfigProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "simple");
    consumerConfigProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerConfigProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString());

    consumerConfigProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");

    // Use environment variable with fallback to localhost for local development
    String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    consumerConfigProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    
    return new DefaultKafkaConsumerFactory<>(consumerConfigProperties);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<Void, byte[]> kafkaListenerContainerFactory() {
    var factory = new ConcurrentKafkaListenerContainerFactory<Void, byte[]>();
    factory.setConsumerFactory(consumerFactory());
    factory.setConcurrency(1);
    factory.setBatchListener(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
  }
}

