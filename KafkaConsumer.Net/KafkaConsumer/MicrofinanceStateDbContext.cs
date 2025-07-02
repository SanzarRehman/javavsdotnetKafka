namespace KafkaConsumer;

using Microsoft.EntityFrameworkCore;

public class MicrofinanceStateDbContext(DbContextOptions<MicrofinanceStateDbContext> options) : DbContext(options)
{
    public DbSet<LoanAggregateRoot> LoanAggregateRoots { get; set; }
    public DbSet<DotnetMessage> DotnetMessages { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder
           .Entity<LoanAggregateRoot>()
           .ToTable("loans");

        modelBuilder
           .Entity<DotnetMessage>()
           .ToTable("dotnet_messages");

        base.OnModelCreating(modelBuilder);
    }
}
