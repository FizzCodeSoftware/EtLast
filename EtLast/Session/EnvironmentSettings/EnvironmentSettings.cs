namespace FizzCode.EtLast;

public sealed class EnvironmentSettings
{
    public SeqSettings SeqSettings { get; } = new SeqSettings();
    public FileLogSettings FileLogSettings { get; } = new FileLogSettings();
    public ConsoleLogSettings ConsoleLogSettings { get; } = new ConsoleLogSettings();

    /// <summary>
    /// Default value is 4 hours.
    /// </summary>
    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromHours(4);
}