namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
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
    }
}