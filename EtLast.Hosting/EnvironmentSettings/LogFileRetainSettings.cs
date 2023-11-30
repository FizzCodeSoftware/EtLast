namespace FizzCode.EtLast;

public sealed class LogFileRetainSettings
{
    /// <summary>
    /// Default value is 30.
    /// </summary>
    public int ImportantFileCount { get; init; } = 30;

    /// <summary>
    /// Default value is 14.
    /// </summary>
    public int InfoFileCount { get; init; } = 14;

    /// <summary>
    /// Default value is 4.
    /// </summary>
    public int LowFileCount { get; init; } = 4;
}
