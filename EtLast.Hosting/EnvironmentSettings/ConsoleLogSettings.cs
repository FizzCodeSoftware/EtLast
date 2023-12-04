namespace FizzCode.EtLast;

public sealed class ConsoleLogSettings
{
    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Default value is <see cref="LogSeverity.Information"/>.
    /// </summary>
    public LogSeverity MinimumLogLevel { get; set; } = LogSeverity.Information;
}