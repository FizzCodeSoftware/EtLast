namespace FizzCode.EtLast
{
    public enum LogSeverity
    {
        Verbose = 0,
        Debug = 1,
        Information = 2,
        Warning = 3,
        Error = 4,
        Fatal = 5,
    }

    public delegate void ContextOnLogDelegate(LogSeverity severity, bool forOps, IProcess process, IBaseOperation operation, string text, params object[] args);
}