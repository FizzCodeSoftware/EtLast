namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;

    internal class LogEntry
    {
        public string[] ContextName { get; set; }
        public string CallerUid { get; set; }
        public string CallerName { get; set; }
        public string OperationType { get; set; }
        public string OperationNumber { get; set; }
        public string OperationName { get; set; }
        public string Text { get; set; }
        public LogSeverity Severity { get; set; }
        public bool ForOps { get; set; }
        public List<object> Arguments { get; set; } = new List<object>();
    }
}