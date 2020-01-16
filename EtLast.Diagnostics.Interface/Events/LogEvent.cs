namespace FizzCode.EtLast.Diagnostics.Interface
{
    using FizzCode.EtLast;

    public class LogEvent : AbstractEvent
    {
        public ProcessInfo Process { get; set; }
        public OperationInfo Operation { get; set; }
        public string Text { get; set; }
        public LogSeverity Severity { get; set; }
        public NamedArgument[] Arguments { get; set; }
        public bool ForOps { get; set; }
    }
}