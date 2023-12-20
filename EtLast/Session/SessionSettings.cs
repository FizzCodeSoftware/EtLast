namespace FizzCode.EtLast;

public class SessionSettings
{
    public required string[] TaskNames { get; init; }

    /// <summary>
    /// Default value is 4 hours.
    /// </summary>
    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromHours(4);

    public List<IManifestProcessor> ManifestProcessors { get; } = [];
}