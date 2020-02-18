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

        public List<AbstractDiagContext> ContextList { get; } = new List<AbstractDiagContext>();
        public Dictionary<string, AbstractDiagContext> ContextListByName { get; } = new Dictionary<string, AbstractDiagContext>();

        public DiagSession(string name, string dataFolder, DateTime startedOn)
        {
            SessionId = name;
            DataFolder = dataFolder;
        }
    }
}