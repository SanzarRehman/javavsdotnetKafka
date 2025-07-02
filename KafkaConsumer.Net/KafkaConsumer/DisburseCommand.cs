namespace KafkaConsumer;

public class DisburseCommand
{
    public double Amount { get; set; } = 1;

    public Guid LoanId { get; set; } = Guid.CreateVersion7();

    public Guid MemberId { get; set; } = Guid.CreateVersion7();

    public Guid CorrelationId { get; set; }
    public UserContext UserContext { get; set; }
}


#region Essentials



public class UserContext
{
    public Guid UserId { get; set; }
    public Guid ApplicationId { get; set; }
    public Guid SessionId { get; set; }
    public Guid TenantId { get; set; }
    public Guid VerticalId { get; set; }
    public string ServiceId { get; set; }
    public string Email { get; set; }
    public string PhoneNumber { get; set; }
    public string UserName { get; set; }
    public string DisplayName { get; set; }
    public string Language { get; set; }
    public List<string> Roles { get; set; }
}

#endregion