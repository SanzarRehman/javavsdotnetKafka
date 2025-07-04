package application.messageListeners;


import application.entities.Loan;
import application.messages.DisburseCommand;
import application.services.DbContextProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class KafkaMessageHandler extends ConcurrentKafkaMessageDispatcher {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DbContextProvider dbContextProvider;
    
    static {
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
    
    public KafkaMessageHandler(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer, DbContextProvider dbContextProvider) {
        super(topicName, concurrency, consumer);
        this.dbContextProvider = dbContextProvider;
    }
    
    @Override
    public CompletableFuture<Void> handleAsync(byte[] message) {
        return CompletableFuture.runAsync(() -> {
            try {
                DisburseCommand command = JsonUtils.getMessage(message, DisburseCommand.class);

                Loan loan = from(command);


                dbContextProvider.save(loan);
                

            } catch (Exception e) {
                throw new RuntimeException("Error processing message", e);
            }
        });
    }

    private static Loan from(DisburseCommand disburseCommand) {
        Loan loan = new Loan();
        loan.setAmount((double) 0);
        loan.setMemberId(UUID.randomUUID());
        loan.setId(UUID.randomUUID());
        loan.setNewEntry(true);
        loan.assignEntityDefaults(disburseCommand.userContext());

        return loan;
    }
}