package application.messageListeners;

import application.messages.DisburseCommand;
import application.services.LoanManagementService;
import application.utils.JsonUtils;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
  private final LoanManagementService loanManagementService;
  private static final AtomicInteger activeTaskCount = new AtomicInteger(0);

  public KafkaMessageListener(LoanManagementService loanManagementService) {
    this.loanManagementService = loanManagementService;
  }

  Semaphore concurrencyLimiter = new Semaphore(50);
  @KafkaListener(topics = "disburse-commands")
  public void listen(ConsumerRecords<String, String> messages, Acknowledgment ack) {
    long startTimeMillis = System.currentTimeMillis();
    System.out.println("Received messages: " + messages.count());

    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
      for (ConsumerRecord<String, String> message : messages) {
        concurrencyLimiter.acquireUninterruptibly(); // blocks if 50 tasks already active

        scope.fork(() -> {
          int currentCount = activeTaskCount.incrementAndGet();
          System.out.println("Active concurrent tasks: " + currentCount);

          try {
            loanManagementService.handleDisburseCommand("ok");
            return null;
          } finally {
            activeTaskCount.decrementAndGet();
            concurrencyLimiter.release(); // allow next task to start
          }
        });
      }

      scope.join(); // wait for all tasks

      long endTimeMillis = System.currentTimeMillis();
      System.out.println("Processing time: " + (endTimeMillis - startTimeMillis) + " ms");

      ack.acknowledge(); // manual commit

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // good practice
      throw new RuntimeException("Interrupted while processing Kafka messages", e);
    }
  }



//  @KafkaListener(topics = "disburse-commands")
//  public void listen(ConsumerRecords<String, byte[]> messages, Acknowledgment ack) {
//    long startTimeMillis = System.currentTimeMillis();
//    System.out.println("Received messages: " + messages.count());
//
//    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
//    List<Future<?>> futures = new ArrayList<>();
//
//    try {
//      for (ConsumerRecord<String, byte[]> message : messages) {
//        futures.add(executor.submit(() -> {
//          try {
//            DisburseCommand command = JsonUtils.getMessage(message.value(), DisburseCommand.class);
//            loanManagementService.handleDisburseCommand(command);
//          } catch (Exception e) {
//            System.err.println("Error processing message with key: " + message.key());
//            e.printStackTrace();
//
//          }
//        }));
//      }
//
//      // Wait for all to complete
//      for (Future<?> future : futures) {
//        future.get(); // throws if any failed
//      }
//
//      long endTimeMillis = System.currentTimeMillis();
//      System.out.println("Processing time: " + (endTimeMillis - startTimeMillis) + " ms");
//
//      ack.acknowledge(); // commit only after all succeeded
//
//    } catch (InterruptedException e) {
//      Thread.currentThread().interrupt();
//      throw new RuntimeException("Interrupted while processing Kafka messages", e);
//
//    } catch (ExecutionException e) {
//      System.err.println("One or more tasks failed: " + e.getCause());
//      throw new RuntimeException("Failure in processing Kafka messages", e.getCause());
//
//    } finally {
//      executor.shutdown(); // always shutdown
//    }
//  }


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
