namespace KafkaConsumer;

using Microsoft.EntityFrameworkCore;

public class DbContextProvider
{
    // Hardcoded connection string for local development
    private readonly string connectionString = "Host=localhost;Database=microfinance_db;Username=postgres;Password=localpass123;Port=5432";

    public MicrofinanceStateDbContext Get()
    {
        var optionsBuilder = new DbContextOptionsBuilder<MicrofinanceStateDbContext>();
        optionsBuilder.UseNpgsql(connectionString);
        optionsBuilder.UseSnakeCaseNamingConvention();

        return new MicrofinanceStateDbContext(optionsBuilder.Options);
    }
}
