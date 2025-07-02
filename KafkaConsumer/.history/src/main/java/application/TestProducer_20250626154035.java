package application;

import application.messages.DisburseCommand;
import application.messages.UserContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestProducer {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            
            System.out.println("🚀 Starting Java Kafka Producer for DisburseCommand testing...");
            System.out.println("📋 Sending test messages every 3 seconds");
            System.out.println("🛑 Press Ctrl+C to stop\n");
            
            int messageCount = 1;
            
            while (true) {
                // Create test UserContext
                UserContext userContext = new UserContext(
                    UUID.randomUUID(), // userId
                    UUID.randomUUID(), // applicationId
                    UUID.randomUUID(), // sessionId
                    UUID.randomUUID(), // tenantId
                    UUID.randomUUID(), // verticalId
                    "loan-service", // serviceId
                    "test@microfinance.com", // email
                    "+1234567890", // phoneNumber
                    "testuser", // userName
                    "Test User", // displayName
                    "en", // language
                    List.of("USER", "LOAN_OFFICER") // roles
                );
                
                // Create test DisburseCommand
                DisburseCommand command = new DisburseCommand(
                    "Loan Disbursement #" + messageCount, // name
                    UUID.randomUUID(), // correlationId
                    userContext // userContext
                );
                
                // Serialize to JSON bytes
                byte[] messageBytes = objectMapper.writeValueAsBytes(command);
                
                // Create producer record
                String key = "disburse-" + messageCount;
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    "loan-disbursement", 
                    key, 
                    messageBytes
                );
                
                // Send message
                final int currentMessageCount = messageCount;
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("❌ Error sending message: " + exception.getMessage());
                    } else {
                        System.out.printf("✅ Sent message %d:%n", currentMessageCount);
                        System.out.printf("   📧 Topic: %s%n", metadata.topic());
                        System.out.printf("   🔑 Key: %s%n", key);
                        System.out.printf("   📝 Name: %s%n", command.name());
                        System.out.printf("   🆔 Correlation ID: %s%n", command.correlationId());
                        System.out.printf("   👤 User: %s%n", command.userContext().userName());
                        System.out.printf("   📍 Partition: %d%n", metadata.partition());
                        System.out.printf("   📊 Offset: %d%n%n", metadata.offset());
                    }
                });
                
                messageCount++;
                
                // Wait 3 seconds
                TimeUnit.SECONDS.sleep(3);
            }
        }
    }
}