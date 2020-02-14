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

        public List<ExecutionContext> ContextList { get; } = new List<ExecutionContext>();
        public Dictionary<string, ExecutionContext> ExecutionContextListByName { get; } = new Dictionary<string, ExecutionContext>();

        public Session(string name, string dataFolder, DateTime startedOn)
        {
            SessionId = name;
            DataFolder = dataFolder;
        }
    }
}