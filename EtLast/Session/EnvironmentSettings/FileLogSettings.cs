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

    /// <summary>
    /// Default value is <see cref="LogSeverity.Verbose"/>.
    /// </summary>
    public LogSeverity MinimumLogLevelIo { get; set; } = LogSeverity.Verbose;

    public LogFileRetainSettings RetainSettings { get; set; } = new LogFileRetainSettings();
}