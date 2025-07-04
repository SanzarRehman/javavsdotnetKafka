namespace KafkaConsumer;

using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

internal class KafkaMessageConsumer(DbContextProvider dbContextProvider) : IHostedService
{
    private const int Concurrency = 400;
    private const string GroupId = "simple";
    private const string TopicName = "disburse-commands";
    private readonly string BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
    
    private readonly List<Task> kafkaListenerTasks = [];
    private readonly CancellationTokenSource cancellationTokenSource = new();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        StartConsumers(TopicName, dbContextProvider, cancellationTokenSource.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            cancellationTokenSource.Cancel();

            await Task.WhenAll(kafkaListenerTasks);
        }

        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }

    private void StartConsumers(string topicName, DbContextProvider dbContextProvider, CancellationToken cancellationToken)
    {
        ConsumerConfig consumerConfig = new()
        {
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            GroupId = GroupId,
            AllowAutoCreateTopics = true,
            BootstrapServers = BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Latest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            MaxPollIntervalMs = 600000, // 10 minutes instead of 5 minutes
            SessionTimeoutMs = 60000,   // 60 seconds
            HeartbeatIntervalMs = 20000, // 20 seconds
            FetchMaxBytes = 1048576,    // 1MB
            MaxPartitionFetchBytes = 1048576 // 1MB per partition
        };

        IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(consumerConfig)
        .Build();

        consumer.Subscribe(topicName);

        KafkaMessageDispatcherBase<string, string> kafkaMessageHandler = Concurrency == 1 ? new OrderedKafkaMessageHandler(topicName, consumer, dbContextProvider) : new KafkaMessageHandler(topicName, Concurrency, consumer, dbContextProvider);

        Task kafkaListenerTask = kafkaMessageHandler.StartAsync(cancellationToken);

        kafkaListenerTasks.Add(kafkaListenerTask);

        Console.WriteLine($"Started Kafka consumer for topic '{topicName}' with concurrency {Concurrency}.");
    }
}
