namespace FizzCode.EtLast.Diagnostics.Interface
{
    using FizzCode.EtLast;

    public class LogEvent
    {
        public string[] ContextName { get; set; }
        public string CallerUid { get; set; }
        public string CallerName { get; set; }
        public string OperationType { get; set; }
        public int? OperationNumber { get; set; }
        public string OperationName { get; set; }
        public string Text { get; set; }
        public LogSeverity Severity { get; set; }
        public Argument[] Arguments { get; set; }
        public bool ForOps { get; set; }
    }
}