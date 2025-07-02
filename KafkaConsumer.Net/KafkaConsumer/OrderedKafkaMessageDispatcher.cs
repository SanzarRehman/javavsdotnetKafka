namespace KafkaConsumer;

using Confluent.Kafka; 

public abstract class OrderedKafkaMessageDispatcher<TKey, TValue>(string topicName, IConsumer<TKey, TValue> consumer) : KafkaMessageDispatcherBase<TKey, TValue>
{
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Starting single threaded no-ops consumer for topic: {topicName}");

        return Task.Run(async () =>
        {
            Task commitSchedulerTask = StartCommitScheduler(consumer, cancellationToken);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<TKey, TValue> consumeResult = consumer.Consume(cancellationToken);

                    await HandleAsync(consumeResult.Message.Key, consumeResult.Message.Value);

                    consumer.StoreOffset(consumeResult);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Message reading canceled.");
            }

            await commitSchedulerTask;
            Console.WriteLine("Commit scheduler stopped successfully.");

            consumer.Close();
            Console.WriteLine("Consumer closed successfully.");

            consumer.Dispose();
            Console.WriteLine("Consumer disposed successfully.");
        });
    }

     

    private Task StartCommitScheduler(IConsumer<TKey, TValue> consumer, CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            TopicPartition topicPartition = new(topicName, 0);

            using PeriodicTimer commitTimer = new(TimeSpan.FromSeconds(5));

            while (!cancellationToken.IsCancellationRequested)
            {
                await commitTimer.WaitForNextTickAsync();

                try
                {
                    consumer.Commit();
                }
                catch (KafkaException e)
                {
                    if (e.Error.Code != ErrorCode.Local_NoOffset)
                    {
                        Console.WriteLine(e);
                    }
                }
            }
        });
    }
}
