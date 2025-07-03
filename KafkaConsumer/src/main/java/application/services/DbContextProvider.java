package application.services;

import application.entities.DotnetMessage;
import application.repositories.DotnetMessageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Service
public class DbContextProvider {
    
    @Autowired
    private DotnetMessageRepository dotnetMessageRepository;
    
    public DotnetMessageRepository getDotnetMessageRepository() {
        return dotnetMessageRepository;
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void save(DotnetMessage message) {
        dotnetMessageRepository.save(message);
    }
}