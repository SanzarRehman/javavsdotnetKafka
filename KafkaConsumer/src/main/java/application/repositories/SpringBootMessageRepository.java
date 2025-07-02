package application.repositories;

import application.entities.SpringBootMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SpringBootMessageRepository extends JpaRepository<SpringBootMessage, Long> {
    // Simple repository - no complex queries or async methods needed
}