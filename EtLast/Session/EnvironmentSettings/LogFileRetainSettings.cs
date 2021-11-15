namespace FizzCode.EtLast
{
    public sealed class LogFileRetainSettings
    {
        public int ImportantFileCount { get; init; } = 30;
        public int InfoFileCount { get; init; } = 14;
        public int LowFileCount { get; init; } = 4;
    }
}