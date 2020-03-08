namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void OnDiagContextStartedOnSetDelegate(AbstractDiagContext diagContext);

    [DebuggerDisplay("{Name}")]
    public abstract class AbstractDiagContext
    {
        public string Name { get; }
        public DiagSession Session { get; }
        public Playbook WholePlaybook { get; }
        public DateTime StartedOn { get; }
        public DateTime? EndedOn { get; protected set; }
        public abstract bool FullyLoaded { get; }
        public Dictionary<int, string> TextDictionary { get; }

        protected AbstractDiagContext(DiagSession session, string name, DateTime startedOn)
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

        public abstract void EnumerateThroughEvents(Func<AbstractEvent, bool> callback, params DiagnosticsEventKind[] eventKindFilter);
        public abstract void EnumerateThroughStoredRows(int storeUid, Action<RowStoredEvent> callback);
    }
}