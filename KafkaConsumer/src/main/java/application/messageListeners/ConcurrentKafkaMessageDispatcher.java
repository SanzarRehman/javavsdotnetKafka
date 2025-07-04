package application.messageListeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConcurrentKafkaMessageDispatcher extends KafkaMessageDispatcherBase {
    
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentKafkaMessageDispatcher.class);

    private final String topicName;
    private final int concurrency;
    private final KafkaConsumer<Void, byte[]> consumer;
    private final TreeSet<Long> completedOffsets = new TreeSet<>();
    private final AtomicLong lastCommittedOffset = new AtomicLong(-1L);
    private final Object lock = new Object();
    private static final AtomicInteger activeTaskCount = new AtomicInteger(0);
    private final BlockingQueue<ConsumerRecord<Void, byte[]>> messageQueue;
    private final ExecutorService executorService;

    protected ConcurrentKafkaMessageDispatcher(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.consumer = consumer;
        this.messageQueue = new LinkedBlockingQueue<>(concurrency * 100);
        this.executorService = Executors.newFixedThreadPool(concurrency + 1, r -> {
            Thread t = new Thread(r);
            t.setName("kafka-message-processor-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }
    
    @Override
    public CompletableFuture<Void> startAsync(AtomicBoolean cancellationToken) {
        logger.info("Starting concurrent Kafka message dispatcher for topic '{}' with concurrency {}", topicName, concurrency);

        // Start dispatch worker tasks
        List<CompletableFuture<Void>> dispatchWorkerTasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            CompletableFuture<Void> task = CompletableFuture.runAsync(this::dispatchMessages, executorService);
            dispatchWorkerTasks.add(task);
        }

        // Consumer polling runs in a single thread and handles commits
        return CompletableFuture.runAsync(() -> {
            long lastCommitTime = System.currentTimeMillis();
            final long COMMIT_INTERVAL_MS = 1000; // 1 second commit interval

            try {
                while (!cancellationToken.get()) {
                    ConsumerRecords<Void, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    
                    for (ConsumerRecord<Void, byte[]> record : records) {
                        if (lastCommittedOffset.get() == -1L) {
                            lastCommittedOffset.set(record.offset() - 1);
                        }
                        
                        try {
                            if (!messageQueue.offer(record, 5, TimeUnit.SECONDS)) {
                                logger.warn("Message queue is full, dropping message with offset {}", record.offset());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    // Perform commit periodically on the same thread
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastCommitTime >= COMMIT_INTERVAL_MS) {
                        performCommit(cancellationToken);
                        lastCommitTime = currentTime;
                    }
                }
            } catch (Exception e) {
                if (!cancellationToken.get()) {
                    logger.error("Error in message consumption", e);
                }
            } finally {
                logger.info("Shutting down Kafka message dispatcher for topic '{}'", topicName);

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
                
                // Close resources
                consumer.close();
                logger.info("Consumer closed successfully");

                shutdownExecutorService(executorService);
            }
        }, executorService);
    }
    
    private void dispatchMessages() {
        try {
            while (true) {
                ConsumerRecord<Void, byte[]> record = messageQueue.take();
                
                if (isPoisonPill(record)) {
                    break;
                }
                
                int currentCount = activeTaskCount.incrementAndGet();
                logger.debug("Active concurrent tasks: {}", currentCount);

                try {
                    handleAsync(record.value()).join();
                    
                    synchronized (lock) {
                        completedOffsets.add(record.offset());
                    }
                } catch (Exception e) {
                    logger.error("Error processing message at offset {}", record.offset(), e);
                } finally {
                    activeTaskCount.decrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Message dispatcher thread interrupted");
        }
    }
    
    private void performCommit(AtomicBoolean cancellationToken) {
        if (cancellationToken.get()) {
            return;
        }

        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        
        try {
            synchronized (lock) {
                long offset = lastCommittedOffset.get();

                while (completedOffsets.contains(offset + 1)) {
                    offset++;
                    completedOffsets.remove(offset);
                }

                if (offset > lastCommittedOffset.get()) {
                    lastCommittedOffset.set(offset);

                    // No need for consumer synchronization since we're on the same thread
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset + 1));
                    consumer.commitSync(offsetsToCommit);

                    logger.debug("Committed offset: {}", offset + 1);
                }
            }
        } catch (Exception e) {
            if (!cancellationToken.get()) {
                logger.error("Error in commit operation", e);
            }
        }
    }

    private void shutdownExecutorService(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Executor did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ConsumerRecord<Void, byte[]> createPoisonPill() {
        return new ConsumerRecord<>(
                "disburse-commands",  // topic
                0,                    // partition
                -1L,                  // offset (use -1 for poison pill)
                null,                 // key (Void)
                new byte[0]           // value (empty byte array)
        );
    }
    
    private boolean isPoisonPill(ConsumerRecord<Void, byte[]> record) {
        return record.offset() == -1L;
    }
    
    public abstract CompletableFuture<Void> handleAsync(byte[] message);
}
