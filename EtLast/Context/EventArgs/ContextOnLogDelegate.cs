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

    public static class LogSeverityHelpers
    {
        public static string ToShortString(this LogSeverity severity)
        {
            return severity switch
            {
                LogSeverity.Verbose => "VRB",
                LogSeverity.Debug => "DBG",
                LogSeverity.Information => "INF",
                LogSeverity.Warning => "WRN",
                LogSeverity.Error => "ERR",
                LogSeverity.Fatal => "FTL",
                _ => severity.ToString(),
            };
        }
    }

    public delegate void ContextOnLogDelegate(LogSeverity severity, bool forOps, IProcess process, IBaseOperation operation, string text, params object[] args);
}