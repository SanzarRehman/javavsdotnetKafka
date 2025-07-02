namespace KafkaConsumer;

using Confluent.Kafka;
using System.Threading;
using System.Threading.Channels;

public abstract class ConcurrentKafkaMessageDispatcher<TKey, TValue>(string topicName, ushort concurrency, IConsumer<TKey, TValue> consumer) : KafkaMessageDispatcherBase<TKey, TValue>
{
    private readonly SortedSet<long> completedOffsets = [];
    private long lastCommittedOffset = -1;
    private readonly Lock locker = new();
    private static int _activeTaskCount = 0;


    private readonly Channel<ConsumeResult<TKey, TValue>> channel = Channel.CreateBounded<ConsumeResult<TKey, TValue>>(new BoundedChannelOptions(concurrency)
    {
        SingleWriter = true,
        SingleReader = concurrency.Equals(1),
        AllowSynchronousContinuations = true,
    });

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        Task[] dispatchWorkerTasks =
            [.. Enumerable
                .Range(0, concurrency)
                .Select(instanceId => DispatchMessagesAsync())];


        return Task.Run(async () =>
        {
            Task commitSchedulerTask = StartCommitScheduler(consumer, cancellationToken);

            try
            {
                ConsumeResult<TKey, TValue> consumeResult = consumer.Consume(cancellationToken);

                lastCommittedOffset = consumeResult.Offset - 1;

                while (!cancellationToken.IsCancellationRequested)
                {
                    await channel.Writer.WriteAsync(consumeResult);

                    consumeResult = consumer.Consume(cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Message reading canceled.");
            }
            finally
            {
                channel.Writer.Complete();
            }

            await Task.WhenAll(dispatchWorkerTasks);

            await commitSchedulerTask;
            Console.WriteLine("Commit scheduler stopped successfully.");

            consumer.Close();
            Console.WriteLine("Consumer closed successfully.");

            consumer.Dispose();
            Console.WriteLine("Consumer disposed successfully.");
        });
    }

    private async Task DispatchMessagesAsync()
    {
        await foreach (ConsumeResult<TKey, TValue> consumeResult in channel.Reader.ReadAllAsync())
        {
            var currentCount = Interlocked.Increment(ref _activeTaskCount);
            Console.WriteLine($"Active concurrent tasks: {currentCount}");

            try
            {
                await HandleAsync(consumeResult.Message.Key, consumeResult.Message.Value);

                lock (locker)
                {
                    completedOffsets.Add(consumeResult.Offset);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                Interlocked.Decrement(ref _activeTaskCount);
            }
        }
    }

    private Task StartCommitScheduler(IConsumer<TKey, TValue> consumer, CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            TopicPartition topicPartition = new(topicName, 0);

            using PeriodicTimer commitTimer = new(TimeSpan.FromSeconds(1));

            while (!cancellationToken.IsCancellationRequested)
            {
                await commitTimer.WaitForNextTickAsync();

                try
                {
                    lock (locker)
                    {
                        long offset = lastCommittedOffset;

                        while (completedOffsets.Contains(offset + 1))
                        {
                            offset++;
                            completedOffsets.Remove(offset);
                        }

                        if (offset > lastCommittedOffset)
                        {
                            lastCommittedOffset = offset;
                            var tpo = new TopicPartitionOffset(topicPartition, new Offset(offset + 1));
                            consumer.Commit(new[] { tpo });

                            //Console.WriteLine($"Committed offset: {offset + 1}");
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        });
    }
}
