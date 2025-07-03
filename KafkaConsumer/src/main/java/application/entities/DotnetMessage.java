package application.entities;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "spring_boot_messages")
public class DotnetMessage {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "message_id")
    private String messageId;
    
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;
    
    @Column(name = "timestamp")
    private LocalDateTime timestamp;
    
    @Column(name = "processed_at")
    private LocalDateTime processedAt;
    
    // Constructors
    public DotnetMessage() {}
    
    public DotnetMessage(String messageId, String content, LocalDateTime timestamp, LocalDateTime processedAt) {
        this.messageId = messageId;
        this.content = content;
        this.timestamp = timestamp;
        this.processedAt = processedAt;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public LocalDateTime getProcessedAt() { return processedAt; }
    public void setProcessedAt(LocalDateTime processedAt) { this.processedAt = processedAt; }
}
