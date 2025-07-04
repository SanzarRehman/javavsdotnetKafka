package application.messageListeners;

import application.services.DbContextProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedKafkaMessageHandler extends KafkaMessageDispatcherBase {
    
    private final String topicName;
    private final KafkaConsumer<Void, byte[]> consumer;
    private final DbContextProvider dbContextProvider;
    
    public OrderedKafkaMessageHandler(String topicName, KafkaConsumer<Void, byte[]> consumer, DbContextProvider dbContextProvider) {
        this.topicName = topicName;
        this.consumer = consumer;
        this.dbContextProvider = dbContextProvider;
    }
    
    @Override
    public CompletableFuture<Void> startAsync(AtomicBoolean cancellationToken) {
        return new KafkaMessageHandler(topicName, 1, consumer, dbContextProvider).startAsync(cancellationToken);
    }
}