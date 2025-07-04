package application.repositories;

import application.entities.Loan;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DotnetMessageRepository extends R2dbcRepository<Loan, Long> {
}