package application.messageListeners;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaMessageDispatcherBase {
    public abstract CompletableFuture<Void> startAsync(AtomicBoolean cancellationToken);
}