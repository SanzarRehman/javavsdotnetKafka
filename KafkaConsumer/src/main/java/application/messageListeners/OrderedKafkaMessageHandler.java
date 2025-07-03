package application.messageListeners;

import application.services.DbContextProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedKafkaMessageHandler extends KafkaMessageDispatcherBase {
    
    private final String topicName;
    private final KafkaConsumer<String, String> consumer;
    private final DbContextProvider dbContextProvider;
    
    public OrderedKafkaMessageHandler(String topicName, KafkaConsumer<String, String> consumer, DbContextProvider dbContextProvider) {
        this.topicName = topicName;
        this.consumer = consumer;
        this.dbContextProvider = dbContextProvider;
    }
    
    @Override
    public CompletableFuture<Void> startAsync(AtomicBoolean cancellationToken) {
        // Implementation for ordered processing (sequential)
        // This delegates to KafkaMessageHandler with concurrency = 1
        return new KafkaMessageHandler(topicName, 1, consumer, dbContextProvider).startAsync(cancellationToken);
    }
}