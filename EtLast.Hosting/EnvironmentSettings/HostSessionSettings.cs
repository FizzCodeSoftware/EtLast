namespace FizzCode.EtLast;

public sealed class HostSessionSettings: SessionSettings
{
    public required string ModuleFolderName { get; init; }
    public required string TasksFolderName { get; init; }
    public required string DevLogFolder { get; init; }
    public required string OpsLogFolder { get; init; }

    public SeqSettings SeqSettings { get; } = new SeqSettings();
    public FileLogSettings FileLogSettings { get; } = new FileLogSettings();
    public ConsoleLogSettings ConsoleLogSettings { get; } = new ConsoleLogSettings();
    public LocalManifestLogSettings LocalManifestLogSettings { get; } = new LocalManifestLogSettings();
}