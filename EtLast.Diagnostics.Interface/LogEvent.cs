namespace FizzCode.EtLast.Diagnostics.Interface
{
    using FizzCode.EtLast;

    public class LogEvent : AbstractEvent
    {
        public string ProcessUid { get; set; }
        public string ProcessName { get; set; }
        public string OperationType { get; set; }
        public int? OperationNumber { get; set; }
        public string OperationName { get; set; }
        public string Text { get; set; }
        public LogSeverity Severity { get; set; }
        public NamedArgument[] Arguments { get; set; }
        public bool ForOps { get; set; }
    }
}