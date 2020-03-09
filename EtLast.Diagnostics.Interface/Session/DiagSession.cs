namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{SessionId}")]
    public class DiagSession
    {
        public string SessionId { get; }
        public string DataFolder { get; }
        public DateTime StartedOn { get; }

        public List<DiagContext> ContextList { get; } = new List<DiagContext>();
        public Dictionary<string, DiagContext> ContextListByName { get; } = new Dictionary<string, DiagContext>();

        public DiagSession(string name, string dataFolder, DateTime startedOn)
        {
            SessionId = name;
            DataFolder = dataFolder;
            StartedOn = startedOn;
        }
    }
}