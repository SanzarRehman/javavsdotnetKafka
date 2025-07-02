package application.services;

import application.entities.Loan;
import application.messages.DisburseCommand;
import application.entities.SpringBootMessage;
import application.repositories.LoanRepository;
import application.repositories.SpringBootMessageRepository;
import jakarta.transaction.Transactional;
import java.util.UUID;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class LoanManagementService {
  private final LoanRepository loanRepository;
  private final SpringBootMessageRepository springBootMessageRepository;

  public LoanManagementService(LoanRepository loanRepository, SpringBootMessageRepository springBootMessageRepository) {
    this.loanRepository = loanRepository;
      this.springBootMessageRepository = springBootMessageRepository;
  }

  @Transactional
 // @Async
  public void handleDisburseCommand(String disburseCommand) {
  //  System.out.println(disburseCommand);
    SpringBootMessage entity = new SpringBootMessage(UUID.randomUUID().toString(), disburseCommand);

    springBootMessageRepository.save(entity);
  }

  private static Loan from(DisburseCommand disburseCommand) {
    Loan loan = new Loan();
    loan.setAmount(disburseCommand.amount());
    loan.setMemberId(disburseCommand.memberId());
    loan.setId(disburseCommand.loanId());
    // Set service_id to identify this as Spring Boot processed
    loan.setServiceId("spring-boot-service");
    loan.assignEntityDefaults(disburseCommand.userContext());

    return loan;
  }
}
