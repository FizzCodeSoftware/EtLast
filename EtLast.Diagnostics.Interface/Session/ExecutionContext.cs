namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void OnExecutionContextStartedOnSetDelegate(ExecutionContext executionContext);

    [DebuggerDisplay("{Name}")]
    public class ExecutionContext
    {
        public string Name { get; }
        public Session Session { get; }
        public Playbook WholePlaybook { get; }
        public DateTime? StartedOn { get; private set; }
        public OnExecutionContextStartedOnSetDelegate OnStartedOnSet { get; set; }

        private readonly List<AbstractEvent> _unprocessedEvents = new List<AbstractEvent>();

        public ExecutionContext(Session session, string name)
        {
            Session = session;
            Name = name;
            WholePlaybook = new Playbook(this);
        }

        public void SetStartedOn(DateTime value)
        {
            if (StartedOn == null)
            {
                StartedOn = value;
                OnStartedOnSet?.Invoke(this);
            }
        }

        public void AddUnprocessedEvents(List<AbstractEvent> newEvents)
        {
            lock (_unprocessedEvents)
            {
                _unprocessedEvents.AddRange(newEvents);
            }
        }

        public void ProcessEvents()
        {
            List<AbstractEvent> newEvents;
            lock (_unprocessedEvents)
            {
                newEvents = new List<AbstractEvent>(_unprocessedEvents);
                _unprocessedEvents.Clear();
            }

            WholePlaybook.AddEvents(newEvents);
        }
    }
}