package application.messageListeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConcurrentKafkaMessageDispatcher extends KafkaMessageDispatcherBase {
    
    private final String topicName;
    private final int concurrency;
    private final KafkaConsumer<String, String> consumer;
    private final TreeSet<Long> completedOffsets = new TreeSet<>();
    private final AtomicLong lastCommittedOffset = new AtomicLong(-1L);
    private final Object lock = new Object();
    private static final AtomicInteger activeTaskCount = new AtomicInteger(0);
    private final BlockingQueue<ConsumerRecord<String, String>> messageQueue;
    
    protected ConcurrentKafkaMessageDispatcher(String topicName, int concurrency, KafkaConsumer<String, String> consumer) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.consumer = consumer;
        this.messageQueue = new LinkedBlockingQueue<>(concurrency);
    }
    
    @Override
    public CompletableFuture<Void> startAsync(AtomicBoolean cancellationToken) {
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        
        // Start dispatch worker tasks
        List<CompletableFuture<Void>> dispatchWorkerTasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            CompletableFuture<Void> task = CompletableFuture.runAsync(this::dispatchMessages, executor);
            dispatchWorkerTasks.add(task);
        }
        
        return CompletableFuture.runAsync(() -> {
            CompletableFuture<Void> commitSchedulerTask = startCommitScheduler(consumer, cancellationToken);
            
            try {
                while (!cancellationToken.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        if (lastCommittedOffset.get() == -1L) {
                            lastCommittedOffset.set(record.offset() - 1);
                        }
                        
                        try {
                            messageQueue.put(record);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                if (!cancellationToken.get()) {
                    System.err.println("Error in message consumption: " + e.getMessage());
                }
            } finally {
                // Signal completion to worker threads
                for (int i = 0; i < concurrency; i++) {
                    try {
                        messageQueue.put(createPoisonPill());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                // Wait for all dispatch tasks to complete
                CompletableFuture.allOf(dispatchWorkerTasks.toArray(new CompletableFuture[0])).join();
                
                // Wait for commit scheduler to complete
                commitSchedulerTask.join();
                System.out.println("Commit scheduler stopped successfully.");
                
                consumer.close();
                System.out.println("Consumer closed successfully.");
                
                executor.shutdown();
            }
        });
    }
    
    private void dispatchMessages() {
        try {
            while (true) {
                ConsumerRecord<String, String> record = messageQueue.take();
                
                if (isPoisonPill(record)) {
                    break;
                }
                
                int currentCount = activeTaskCount.incrementAndGet();
                System.out.println("Active concurrent tasks: " + currentCount);
                
                try {
                    handleAsync(record.key(), record.value()).join();
                    
                    synchronized (lock) {
                        completedOffsets.add(record.offset());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                } finally {
                    activeTaskCount.decrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private CompletableFuture<Void> startCommitScheduler(KafkaConsumer<String, String> consumer, AtomicBoolean cancellationToken) {
        return CompletableFuture.runAsync(() -> {
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            
            while (!cancellationToken.get()) {
                try {
                    Thread.sleep(1000); // 1 second interval
                    
                    synchronized (lock) {
                        long offset = lastCommittedOffset.get();
                        
                        while (completedOffsets.contains(offset + 1)) {
                            offset++;
                            completedOffsets.remove(offset);
                        }
                        
                        if (offset > lastCommittedOffset.get()) {
                            lastCommittedOffset.set(offset);
                            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                            offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset + 1));
                            consumer.commitSync(offsetsToCommit);
                            
                            // Uncomment for debugging
                            // System.out.println("Committed offset: " + (offset + 1));
                        }
                    }
                } catch (Exception e) {
                    if (!cancellationToken.get()) {
                        System.err.println("Error in commit scheduler: " + e.getMessage());
                    }
                }
            }
        });
    }
    
    private ConsumerRecord<String, String> createPoisonPill() {
        return new ConsumerRecord<>("", 0, -1L, "", "");
    }
    
    private boolean isPoisonPill(ConsumerRecord<String, String> record) {
        return record.offset() == -1L;
    }
    
    public abstract CompletableFuture<Void> handleAsync(String key, String message);
}