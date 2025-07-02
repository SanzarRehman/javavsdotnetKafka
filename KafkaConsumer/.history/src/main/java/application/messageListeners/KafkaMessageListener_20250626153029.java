package application.messageListeners;

import application.messages.DisburseCommand;
import application.services.LoanManagementService;
import java.util.concurrent.StructuredTaskScope;
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

  @KafkaListener(topics = "loan-disbursement")
  public void listen(ConsumerRecords<Void, byte[]> messages, Acknowledgment ack) {

    System.out.println(messages.count());
    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
      for (ConsumerRecord<Void, byte[]> message : messages) {
        scope.fork(() -> {
          DisburseCommand command = JsonUtils.getMessage(message.value(), DisburseCommand.class);
          loanManagementService.handleDisburseCommand(command);
          return null;
        });
      }

      scope.join();

      ack.acknowledge();

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }



/*  @KafkaListener(topics = "Microfinance.Commands.DisburseCommand")
  public void listen(ConsumerRecords<Void, byte[]> messages, Acknowledgment ack)
      throws InterruptedException, IOException {

    // We should offload CPU bound tasks (Json deserialization) as much as possible from the Virtual Thread context.
    // Therefore, we are preparing the commands from the incoming byte[]'s to DisburseCommand[] outside the callables.

    int index = 0;
    DisburseCommand[] commands = new DisburseCommand[messages.count()];

    for (ConsumerRecord<Void, byte[]> message : messages) {
      commands[index++] = JsonUtils.getMessage(message.value(), DisburseCommand.class);
    }

    // Preparing the callables only with instructions mostly bounded to I/O
    List<Callable<Void>> tasks = new ArrayList<>();
    for (DisburseCommand command : commands) {
      tasks.add(() -> {
        loanManagementService.handleDisburseCommand(command);
        return null;
      });
    }

    // Try to run the tasks in parallel and awaits completions. Intentionally Ignoring Errors - FIRE AND FORGET.
    executorService.invokeAll(tasks);

    // Acknowledge the batch
    ack.acknowledge();
  }*/


}

