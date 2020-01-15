namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{FullName}")]
    public class SessionContext
    {
        public string Name { get; }

        public DiagnosticsSession Session { get; }
        public SessionContext ParentContext { get; }
        public List<SessionContext> ChildContextList { get; } = new List<SessionContext>();
        public Dictionary<string, SessionContext> ChildContextListByName { get; } = new Dictionary<string, SessionContext>();
        public Playbook Playbook { get; }

        public SessionContext(DiagnosticsSession session, string name, SessionContext parentContext = null)
        {
            Session = session;
            Name = name;
            Playbook = new Playbook(this);
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

                    _fullName = Session.SessionId + "/" + Name;
                }

                return _fullName;
            }
        }
    }
}