package application.services;

import application.entities.SpringBootMessage;
import application.repositories.SpringBootMessageRepository;
import application.messages.DisburseCommand;
import application.utils.JsonUtils;
import jakarta.transaction.Transactional;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class LoanManagementService {
  private final SpringBootMessageRepository springBootMessageRepository;

  public LoanManagementService(SpringBootMessageRepository springBootMessageRepository) {
    this.springBootMessageRepository = springBootMessageRepository;
  }

  // Simple async method matching .NET's approach
  @Async("taskExecutor")
  @Transactional
  public CompletableFuture<Void> handleDisburseCommandAsync(String disburseCommand) {
    SpringBootMessage entity = new SpringBootMessage(
            UUID.randomUUID().toString(),
            disburseCommand
    );

    springBootMessageRepository.save(entity);
    return CompletableFuture.completedFuture(null);
  }
}
