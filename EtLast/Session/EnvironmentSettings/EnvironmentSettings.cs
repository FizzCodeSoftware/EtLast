namespace FizzCode.EtLast;

using System;

public sealed class EnvironmentSettings
{
    public SeqSettings SeqSettings { get; } = new SeqSettings();
    public FileLogSettings FileLogSettings { get; } = new FileLogSettings();
    public ConsoleLogSettings ConsoleLogSettings { get; } = new ConsoleLogSettings();

    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(60);
}
