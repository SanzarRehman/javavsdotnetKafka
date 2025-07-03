package application.repositories;

import application.entities.DotnetMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DotnetMessageRepository extends JpaRepository<DotnetMessage, Long> {
}