namespace KafkaConsumer;

using Confluent.Kafka;
using System.Text.Json;

internal class KafkaMessageHandler(string topicName, ushort concurrency, IConsumer<string, string> consumer, DbContextProvider dbContextProvider)
    : ConcurrentKafkaMessageDispatcher<string, string>(topicName, concurrency, consumer)
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public override async ValueTask HandleAsync(string key, string message)
    {
        // You can now access both the message key and value
      //  Console.WriteLine($"Processing message with ke22: {key}");

        DisburseCommand disburseCommand = JsonSerializer.Deserialize<DisburseCommand>(message, JsonSerializerOptions);

        // Set service_id to identify this as .NET processed
        disburseCommand.UserContext.ServiceId = "dotnet-service";
        
        
        DotnetMessage dotnetMessage = new()
        {
            MessageId = key ?? Guid.NewGuid().ToString(),
            Content = message,
            Timestamp = DateTime.UtcNow,
            ProcessedAt = DateTime.UtcNow
        };

        using MicrofinanceStateDbContext dbContext = dbContextProvider.Get();

        // Save to both tables
  //      dbContext.LoanAggregateRoots.Add(newLoanAggregateRoot);
        dbContext.DotnetMessages.Add(dotnetMessage);

        await dbContext.SaveChangesAsync();
    }
}