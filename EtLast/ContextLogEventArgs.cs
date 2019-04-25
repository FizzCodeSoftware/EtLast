namespace FizzCode.EtLast
{
    using System;

    public enum LogSeverity { Verbose, Debug, Information, Warning, Error, Ops }

    public class ContextLogEventArgs : EventArgs
    {
        public IProcess Process { get; set; }
        public string Text { get; set; }
        public object[] Arguments { get; set; }
        public LogSeverity Severity { get; set; }
        public bool ForOps { get; set; } = false;
    }
}