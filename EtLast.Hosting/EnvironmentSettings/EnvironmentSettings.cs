namespace FizzCode.EtLast;

public sealed class EnvironmentSettings
{
    public required string DevLogFolder { get; init; }
    public required string OpsLogFolder { get; init; }

    public SeqSettings SeqSettings { get; } = new SeqSettings();
    public FileLogSettings FileLogSettings { get; } = new FileLogSettings();
    public ConsoleLogSettings ConsoleLogSettings { get; } = new ConsoleLogSettings();
    public LocalManifestLogSettings LocalManifestLogSettings { get; } = new LocalManifestLogSettings();

    /// <summary>
    /// Default value is 4 hours.
    /// </summary>
    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromHours(4);

    public List<IManifestProcessor> ManifestProcessors { get; } = [];
}