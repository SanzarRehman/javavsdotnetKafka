namespace KafkaConsumer;

using Microsoft.EntityFrameworkCore;

public class DbContextProvider
{
    private readonly string connectionString;

    public DbContextProvider()
    {
        // Use the DB_CONNECTION_STRING environment variable provided by Docker Compose
        // or fall back to individual environment variables, or finally to default values
        connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING") 
            ?? BuildConnectionStringFromEnvVars();
    }

    private string BuildConnectionStringFromEnvVars()
    {
        var dbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "postgres";
        var dbPort = Environment.GetEnvironmentVariable("DB_PORT") ?? "5432";
        var dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "microfinance_db";
        var dbUsername = Environment.GetEnvironmentVariable("DB_USERNAME") ?? "postgres";
        var dbPassword = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "localpass123";

        return $"Host={dbHost};Database={dbName};Username={dbUsername};Password={dbPassword};Port={dbPort}";
    }

    public MicrofinanceStateDbContext Get()
    {
        var optionsBuilder = new DbContextOptionsBuilder<MicrofinanceStateDbContext>();
        optionsBuilder.UseNpgsql(connectionString);
        optionsBuilder.UseSnakeCaseNamingConvention();

        return new MicrofinanceStateDbContext(optionsBuilder.Options);
    }
}
