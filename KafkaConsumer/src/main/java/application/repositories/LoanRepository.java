package application.repositories;

import application.entities.Loan;
import java.util.UUID;
import org.springframework.data.repository.CrudRepository;

public interface LoanRepository extends CrudRepository<Loan, UUID> {
}
