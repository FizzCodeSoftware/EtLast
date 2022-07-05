namespace FizzCode.EtLast;

public sealed class ConsoleLogSettings
{
    public bool Enabled { get; set; } = true;
    public LogSeverity MinimumLogLevel { get; set; } = LogSeverity.Debug;
}