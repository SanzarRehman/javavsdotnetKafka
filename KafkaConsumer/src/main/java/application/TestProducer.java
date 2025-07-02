// package application;

// import application.messages.DisburseCommand;
// import application.messages.UserContext;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.clients.consumer.OffsetResetStrategy;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.common.serialization.*;

// import java.util.List;
// import java.util.Properties;
// import java.util.UUID;
// import java.util.concurrent.TimeUnit;

// public class TestProducer {
    
//     private static final ObjectMapper objectMapper = new ObjectMapper();
    
//     public static void main(String[] args) throws Exception {
//         // Configure Kafka producer
//         Properties consumerConfigProperties = new Properties();
//         consumerConfigProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "simple");
//         consumerConfigProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//         consumerConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//         consumerConfigProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//         consumerConfigProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
//         consumerConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//         consumerConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString());

//         consumerConfigProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

//         consumerConfigProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
//                 "localhost:9092");
        
//         try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(consumerConfigProperties)) {
            
//             System.out.println("🚀 Starting Java Kafka Producer for DisburseCommand testing...");
//             System.out.println("📋 Sending test messages every 3 seconds");
//             System.out.println("🛑 Press Ctrl+C to stop\n");
            
//             int messageCount = 1;

//             while (true) {
//                 // Create test UserContext
//                 UserContext userContext = new UserContext(
//                     UUID.randomUUID(), // userId
//                     UUID.randomUUID(), // applicationId
//                     UUID.randomUUID(), // sessionId
//                     UUID.randomUUID(), // tenantId
//                     UUID.randomUUID(), // verticalId
//                     "loan-service", // serviceId
//                     "test@microfinance.com", // email
//                     "+1234567890", // phoneNumber
//                     "testuser", // userName
//                     "Test User", // displayName
//                     "en", // language
//                     List.of("USER", "LOAN_OFFICER") // roles
//                 );
                
//                 // Create test DisburseCommand with new format
//                 DisburseCommand command = new DisburseCommand(
//                     1000.0 + (Math.random() * 9000.0), // amount (double) - random amount between 1000-10000
//                     UUID.randomUUID(), // loanId
//                     UUID.randomUUID(), // memberId
//                     UUID.randomUUID(), // correlationId
//                     userContext // userContext
//                 );
                
//                 // Serialize to JSON bytes
//                 byte[] messageBytes = objectMapper.writeValueAsBytes(command);
                
//                 // Create producer record
//                 String key = "disburse-" + messageCount;
//                 ProducerRecord<String, byte[]> record = new ProducerRecord<>(
//                     "disburse-commands", // topic
//                     key, // key
//                     messageBytes // value
//                 );
//                 System.out.printf("✅ Sent message");
//                 // Send message
//                 final int currentMessageCount = messageCount;
//                 producer.send(record, (metadata, exception) -> {
//                     if (exception != null) {
//                         System.err.println("❌ Error sending message: " + exception.getMessage());
//                     } else {
//                         System.out.printf("✅ Sent message %d:%n", currentMessageCount);
//                     }
//                 });
                
//                 messageCount++;
//             }
//         }
//     }
// }