namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{SessionId}")]
    public class Session
    {
        public string SessionId { get; }

        public List<ExecutionContext> ContextList { get; } = new List<ExecutionContext>();
        public Dictionary<string, ExecutionContext> ExecutionContextListByName { get; } = new Dictionary<string, ExecutionContext>();

        public Session(string name)
        {
            SessionId = name;
        }
    }
}