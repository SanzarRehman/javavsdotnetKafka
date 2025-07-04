package application.services;


import application.entities.Loan;
import application.repositories.DotnetMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DbContextProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(DbContextProvider.class);

    @Autowired
    private DotnetMessageRepository dotnetMessageRepository;

    public void save(Loan message) {
        if (message == null) {
            logger.warn("Attempted to save null message");
            return;
        }

        try {
            dotnetMessageRepository.save(message)
                .doOnSuccess(savedMessage -> logger.debug("Successfully saved message with ID:"))
                .doOnError(error -> logger.error("Failed to save message with ID: {}", message, error))
                .then()
                .subscribe();
        } catch (Exception e) {
            logger.error("Error saving message to database", e);
            throw new RuntimeException("Database save operation failed", e);
        }
    }
}