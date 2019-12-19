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

        private readonly List<SessionContext> _createdContexts = new List<SessionContext>();

        private SessionContext GetContext(string[] names)
        {
            _createdContexts.Clear();

            if (!ContextListByName.TryGetValue(names[0], out var context))
            {
                context = new SessionContext(this, names[0], null);
                ContextList.Add(context);
                ContextListByName.Add(names[0], context);
                _createdContexts.Add(context);
            }

            for (var i = 1; i < names.Length; i++)
            {
                if (!context.ChildContextListByName.TryGetValue(names[i], out var childContext))
                {
                    childContext = new SessionContext(this, names[i], context);
                    context.ChildContextList.Add(childContext);
                    context.ChildContextListByName.Add(names[i], childContext);
                    _createdContexts.Add(childContext);
                }

                context = childContext;
            }

            foreach (var ctx in _createdContexts)
            {
                OnSessionContextCreated?.Invoke(ctx);
            }

            return context;
        }

        public SessionContext AddEvent(AbstractEvent evt)
        {
            var context = GetContext(evt.ContextName);
            context.WholePlaybook.AddEvent(evt);
            return context;
        }

        public IEnumerable<SessionContext> GetAllLeafContext()
        {
            return GetAllLeafContextRecursive(ContextList);
        }

        private IEnumerable<SessionContext> GetAllLeafContextRecursive(List<SessionContext> list)
        {
            foreach (var context in list)
            {
                if (context.ChildContextList?.Count > 0)
                {
                    foreach (var child in GetAllLeafContextRecursive(context.ChildContextList))
                    {
                        yield return child;
                    }
                }
                else
                {
                    yield return context;
                }
            }
        }
    }
}