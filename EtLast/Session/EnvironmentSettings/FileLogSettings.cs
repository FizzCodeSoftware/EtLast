namespace FizzCode.EtLast;

public sealed class FileLogSettings
{
    public bool Enabled { get; set; } = true;
    public LogSeverity MinimumLogLevel { get; set; } = LogSeverity.Debug;
    public LogSeverity MinimumLogLevelIo { get; set; } = LogSeverity.Verbose;
    public LogFileRetainSettings RetainSettings { get; set; } = new LogFileRetainSettings();
}