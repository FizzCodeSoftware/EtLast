namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{SessionId}")]
    public class DiagnosticsSession
    {
        public string SessionId { get; }

        public List<SessionContext> ContextList { get; } = new List<SessionContext>();
        public Dictionary<string, SessionContext> ContextListByName { get; } = new Dictionary<string, SessionContext>();

        public DiagnosticsSession(string name)
        {
            SessionId = name;
        }

        private SessionContext GetContext(string[] names)
        {
            if (!ContextListByName.TryGetValue(names[0], out var context))
            {
                context = new SessionContext(this, names[0], null);
                ContextList.Add(context);
                ContextListByName.Add(names[0], context);
            }

            for (var i = 1; i < names.Length; i++)
            {
                if (!context.ChildContextListByName.TryGetValue(names[i], out var childContext))
                {
                    childContext = new SessionContext(this, names[i], context);
                    context.ChildContextList.Add(childContext);
                    context.ChildContextListByName.Add(names[i], childContext);
                }

                context = childContext;
            }

            return context;
        }

        public SessionContext AddEvent(AbstractEvent evt)
        {
            var context = GetContext(evt.ContextName);
            context.Playbook.AddEvent(evt);
            return context;
        }
    }
}