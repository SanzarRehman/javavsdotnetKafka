namespace KafkaConsumer;

using Confluent.Kafka;
using System.Text.Json;

internal class OrderedKafkaMessageHandler(string topicName, IConsumer<string, string> consumer, DbContextProvider dbContextProvider)
    : OrderedKafkaMessageDispatcher<string, string>(topicName, consumer)
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public override async ValueTask HandleAsync(string key, string message)
    {
        // You can now access both the message key and value
        Console.WriteLine($"Processing ordered message with key: {key}");

        DisburseCommand disburseCommand = JsonSerializer.Deserialize<DisburseCommand>(message, JsonSerializerOptions);

        // Set service_id to identify this as .NET processed
        disburseCommand.UserContext.ServiceId = "dotnet-service";

        LoanAggregateRoot newLoanAggregateRoot = new();

        newLoanAggregateRoot.CreateLoan(disburseCommand.UserContext);

        // Create a new DotnetMessage record
        DotnetMessage dotnetMessage = new()
        {
            MessageId = key ?? Guid.NewGuid().ToString(), // Use message key as MessageId, or generate one if null
            Content = message,
            Timestamp = DateTime.UtcNow,
            ProcessedAt = DateTime.UtcNow
        };

        using MicrofinanceStateDbContext dbContext = dbContextProvider.Get();

        // Save to both tables
      //  dbContext.LoanAggregateRoots.Add(newLoanAggregateRoot);
        dbContext.DotnetMessages.Add(dotnetMessage);

        await dbContext.SaveChangesAsync();
    }
}