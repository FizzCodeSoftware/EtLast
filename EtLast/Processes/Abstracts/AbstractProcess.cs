namespace FizzCode.EtLast
{
    using System.ComponentModel;
    using System.Linq;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractProcess : IProcess
    {
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public ProcessInvocationInfo InvocationInfo { get; set; }

        public IEtlContext Context { get; }

        public string Topic { get; set; }
        public string Name { get; set; }

        public string Kind { get; }

        protected AbstractProcess(IEtlContext context, string topic, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name ?? GetType().GetFriendlyTypeName();
            Topic = topic;
            Kind = GetProcessKind(this);
        }

        private static string GetProcessKind(IProcess process)
        {
            if (process.GetType().GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IExecutableWithResult<>)))
                return "jobWithResult";

            return process switch
            {
                IRowSource _ => "source",
                IRowSink _ => "sink",
                IMutator _ => "mutator",
                IScope _ => "scope",
                IEvaluable _ => "producer",
                IExecutable _ => "job",
                _ => "unknown",
            };
        }

        public override string ToString()
        {
            var typeName = GetType().GetFriendlyTypeName();
            return typeName + (Name != typeName ? " (" + Name + ")" : "");
        }
    }
}