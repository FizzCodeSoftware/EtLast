namespace FizzCode.EtLast;

public sealed class FileLogSettings
{
    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Default value is <see cref="LogSeverity.Debug"/>.
    /// </summary>
    public LogSeverity MinimumLogLevel { get; set; } = LogSeverity.Debug;

    public LogFileRetainSettings RetainSettings { get; set; } = new LogFileRetainSettings();
}