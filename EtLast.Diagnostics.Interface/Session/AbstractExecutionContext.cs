namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void OnExecutionContextStartedOnSetDelegate(AbstractExecutionContext executionContext);

    [DebuggerDisplay("{Name}")]
    public abstract class AbstractExecutionContext
    {
        public string Name { get; }
        public Session Session { get; }
        public Playbook WholePlaybook { get; }
        public DateTime StartedOn { get; }
        public DateTime? EndedOn { get; protected set; }
        public Dictionary<int, string> TextDictionary { get; }

        protected AbstractExecutionContext(Session session, string name, DateTime startedOn)
        {
            Session = session;
            Name = name;
            WholePlaybook = new Playbook(this);
            StartedOn = startedOn;
            TextDictionary = new Dictionary<int, string>()
            {
                [0] = null,
            };
        }

        public abstract void EnumerateThroughEvents(Action<AbstractEvent> callback, params DiagnosticsEventKind[] eventKindFilter);
        public abstract void EnumerateThroughStoredRows(int storeUid, Action<RowStoredEvent> callback);
    }
}