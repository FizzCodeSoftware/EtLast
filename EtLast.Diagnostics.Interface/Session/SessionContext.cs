namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{FullName}")]
    public class SessionContext
    {
        public string Name { get; }

        public Session Session { get; }
        public SessionContext ParentContext { get; }
        public List<SessionContext> ChildContextList { get; } = new List<SessionContext>();
        public Dictionary<string, SessionContext> ChildContextListByName { get; } = new Dictionary<string, SessionContext>();
        public Playbook WholePlaybook { get; }

        public SessionContext(Session session, string name, SessionContext parentContext = null)
        {
            Session = session;
            Name = name;
            WholePlaybook = new Playbook(this);
            ParentContext = parentContext;
        }

        private string _fullName;

        public string FullName
        {
            get
            {
                if (_fullName == null)
                {
                    if (ParentContext != null)
                        return ParentContext.FullName + "/" + Name;

                    _fullName = Name;
                }

                return _fullName;
            }
        }
    }
}