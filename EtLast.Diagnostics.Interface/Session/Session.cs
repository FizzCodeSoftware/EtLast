namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{SessionId}")]
    public class Session
    {
        public string SessionId { get; }
        public string DataFolder { get; }
        public DateTime StartedOn { get; }

        public List<AbstractExecutionContext> ContextList { get; } = new List<AbstractExecutionContext>();
        public Dictionary<string, AbstractExecutionContext> ExecutionContextListByName { get; } = new Dictionary<string, AbstractExecutionContext>();

        public Session(string name, string dataFolder, DateTime startedOn)
        {
            SessionId = name;
            DataFolder = dataFolder;
        }
    }
}