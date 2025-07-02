namespace KafkaConsumer;

public class LoanAggregateRoot
{
    public Guid MemberId { get; set; }
    public double Amount { get; set; }
    public Guid Id { get; set; }
    public Guid CreatedBy { get; set; }
    public DateTime CreatedDate { get; set; }
    public string Language { get; set; }
    public DateTime LastUpdatedDate { get; set; }
    public Guid LastUpdatedBy { get; set; }
    public Guid TenantId { get; set; }
    public Guid VerticalId { get; set; }
    public string ServiceId { get; set; }
    public bool IsMarkedToDelete { get; set; }
    public int Version { get; set; }

    public void CreateLoan(UserContext userContext)
    {
        DateTime currentTime = DateTime.UtcNow;

        Id = Guid.CreateVersion7();
        MemberId = Guid.CreateVersion7();
        Amount = 1;
        Version = 0;
        IsMarkedToDelete = false;

        CreatedDate = currentTime;
        CreatedBy = userContext.UserId;
        Language = userContext.Language;
        TenantId = userContext.TenantId;
        ServiceId = userContext.ServiceId;
        VerticalId = userContext.VerticalId;
        LastUpdatedDate = currentTime;
        LastUpdatedBy = userContext.UserId;
    }

    public void UpdateAmount(double amount)
    {
        Amount = amount;
    }
}



