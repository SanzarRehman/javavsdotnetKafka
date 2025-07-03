package application.messageListeners;

import application.entities.DotnetMessage;
import application.messages.DisburseCommand;
import application.services.DbContextProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class KafkaMessageHandler extends ConcurrentKafkaMessageDispatcher {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DbContextProvider dbContextProvider;
    
    static {
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
    
    public KafkaMessageHandler(String topicName, int concurrency, KafkaConsumer<String, String> consumer, DbContextProvider dbContextProvider) {
        super(topicName, concurrency, consumer);
        this.dbContextProvider = dbContextProvider;
    }
    
    @Override
    public CompletableFuture<Void> handleAsync(String key, String message) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Uncomment for debugging
                // System.out.println("Processing message with key: " + key);
                
                String disburseCommand = message;

                
                DotnetMessage dotnetMessage = new DotnetMessage();
                dotnetMessage.setMessageId(key != null ? key : UUID.randomUUID().toString());
                dotnetMessage.setContent(disburseCommand);
                dotnetMessage.setTimestamp(LocalDateTime.now());
                dotnetMessage.setProcessedAt(LocalDateTime.now());
                
                // Add 1 second sleep before database save (uncomment if needed)
                // Thread.sleep(1000);
                
                // Save to database
                dbContextProvider.save(dotnetMessage);
                
            } catch (Exception e) {
                throw new RuntimeException("Error processing message", e);
            }
        });
    }
}