namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void OnSessionContextCreatedDelegate(ExecutionContext executionContext);

    [DebuggerDisplay("{SessionId}")]
    public class Session
    {
        public string SessionId { get; }

        public List<ExecutionContext> ContextList { get; } = new List<ExecutionContext>();
        public Dictionary<string, ExecutionContext> ExecutionContextListByName { get; } = new Dictionary<string, ExecutionContext>();
        public OnSessionContextCreatedDelegate OnExecutionContextCreated { get; set; }

        public Session(string name)
        {
            SessionId = name;
        }

        public ExecutionContext GetExecutionContext(string name)
        {
            if (name == null)
                name = "/";

            if (!ExecutionContextListByName.TryGetValue(name, out var context))
            {
                context = new ExecutionContext(this, name);
                ContextList.Add(context);
                ExecutionContextListByName.Add(name, context);

                OnExecutionContextCreated?.Invoke(context);
            }

            return context;
        }
    }
}