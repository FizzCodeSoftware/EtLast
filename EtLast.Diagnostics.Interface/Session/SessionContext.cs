namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Diagnostics;

    [DebuggerDisplay("{FullName}")]
    public class SessionContext
    {
        public string Name { get; }
        public Session Session { get; }
        public Playbook WholePlaybook { get; }

        public SessionContext(Session session, string name)
        {
            Session = session;
            Name = name;
            WholePlaybook = new Playbook(this);
        }
    }
}