namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void OnSessionContextCreatedDelegate(SessionContext sessionContext);

    [DebuggerDisplay("{SessionId}")]
    public class Session
    {
        public string SessionId { get; }

        public List<SessionContext> ContextList { get; } = new List<SessionContext>();
        public Dictionary<string, SessionContext> ContextListByName { get; } = new Dictionary<string, SessionContext>();
        public OnSessionContextCreatedDelegate OnSessionContextCreated { get; set; }

        public Session(string name)
        {
            SessionId = name;
        }

        public SessionContext GetContext(string name)
        {
            if (name == null)
                name = "/";

            if (!ContextListByName.TryGetValue(name, out var context))
            {
                context = new SessionContext(this, name);
                ContextList.Add(context);
                ContextListByName.Add(name, context);

                OnSessionContextCreated?.Invoke(context);
            }

            return context;
        }
    }
}