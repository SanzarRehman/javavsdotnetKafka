package application.services;

import application.messageListeners.KafkaMessageDispatcherBase;
import application.messageListeners.KafkaMessageHandler;
import application.messageListeners.OrderedKafkaMessageHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class KafkaMessageConsumer {
    
    private static final int CONCURRENCY = 50;
    private static final String GROUP_ID = "simple";
    private static final String TOPIC_NAME = "disburse-commands";
    
    @Autowired
    private DbContextProvider dbContextProvider;
    
    private final List<CompletableFuture<Void>> kafkaListenerTasks = new ArrayList<>();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    
    @PostConstruct
    public void startAsync() {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        startConsumers(TOPIC_NAME, dbContextProvider, bootstrapServers);
    }
    
    @PreDestroy
    public void stopAsync() {
        try {
            cancellationRequested.set(true);
            CompletableFuture.allOf(kafkaListenerTasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            System.err.println("Error stopping consumers: " + e.getMessage());
        }
    }
    
    private void startConsumers(String topicName, DbContextProvider dbContextProvider, String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000); // 10 minutes
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000); // 60 seconds
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 20000); // 20 seconds
        consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1048576); // 1MB
        consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576); // 1MB per partition
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topicName));
        
        KafkaMessageDispatcherBase messageHandler;
        if (CONCURRENCY == 1) {
            messageHandler = new OrderedKafkaMessageHandler(topicName, consumer, dbContextProvider);
        } else {
            messageHandler = new KafkaMessageHandler(topicName, CONCURRENCY, consumer, dbContextProvider);
        }
        
        CompletableFuture<Void> kafkaListenerTask = messageHandler.startAsync(cancellationRequested);
        kafkaListenerTasks.add(kafkaListenerTask);
        
        System.out.println("Started Kafka consumer for topic '" + topicName + "' with concurrency " + CONCURRENCY + ".");
    }
}