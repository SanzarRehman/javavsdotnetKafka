package application;


import application.messages.DisburseCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

public class DisburseCommandProducer {
    private final KafkaTemplate<Void, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DisburseCommandProducer(KafkaTemplate<Void, byte[]> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendDisburseCommand(DisburseCommand command) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(command);
            kafkaTemplate.send("Microfinance.Commands.DisburseCommand", payload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send DisburseCommand", e);
        }
    }
}