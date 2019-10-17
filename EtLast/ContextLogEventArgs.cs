namespace FizzCode.EtLast
{
    using System;

    public enum LogSeverity
    {
        Verbose,
        Debug,
        Information,
        Warning,
        Error,
        Fatal
    }

    public class ContextLogEventArgs : EventArgs
    {
        public ICaller Caller { get; set; }
        public IJob Job { get; set; }
        public IBaseOperation Operation { get; set; }
        public string Text { get; set; }
        public object[] Arguments { get; set; }
        public LogSeverity Severity { get; set; }
        public bool ForOps { get; set; }
    }
}