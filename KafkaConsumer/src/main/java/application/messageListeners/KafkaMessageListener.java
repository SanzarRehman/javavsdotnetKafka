package application.messageListeners;

import application.messages.DisburseCommand;
import application.services.LoanManagementService;
import application.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
  private final LoanManagementService loanManagementService;

  public KafkaMessageListener(LoanManagementService loanManagementService) {
    this.loanManagementService = loanManagementService;
  }

  // Simple implementation matching .NET's approach - no batch processing, no TPS printing
  @KafkaListener(topics = "disburse-commands")
  public void listen(ConsumerRecords<String, String> messages, Acknowledgment ack) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (ConsumerRecord<String, String> message : messages) {
      try {
      //  DisburseCommand command = JsonUtils.getMessage(message.value(), DisburseCommand.class);
        CompletableFuture<Void> future = loanManagementService.handleDisburseCommandAsync(message.value());
        futures.add(future);
      } catch (Exception e) {
        System.err.println("Error processing message: " + e.getMessage());
      }
    }

    try {
      // Wait for all async operations to complete
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      ack.acknowledge();
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Error in async message processing", e);
    }
  }
}
