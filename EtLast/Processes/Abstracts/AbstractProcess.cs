namespace FizzCode.EtLast
{
    using System.Linq;

    public abstract class AbstractProcess : IProcess
    {
        public ProcessInvocationInfo InvocationInfo { get; set; }

        public IEtlContext Context => Topic.Context;
        public ITopic Topic { get; set; }
        public string Name { get; set; }

        public StatCounterCollection CounterCollection { get; }

        public ProcessKind Kind { get; }

        protected AbstractProcess(ITopic topic, string name)
        {
            Topic = topic ?? throw new ProcessParameterNullException(this, nameof(topic));
            Name = name ?? GetType().GetFriendlyTypeName();
            Topic = topic;
            CounterCollection = new StatCounterCollection(Context.CounterCollection);
            Kind = GetProcessKind(this);
        }

        private static ProcessKind GetProcessKind(IProcess process)
        {
            if (process.GetType().GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IExecutableWithResult<>)))
                return ProcessKind.jobWithResult;

            return process switch
            {
                IRowReader _ => ProcessKind.reader,
                IRowWriter _ => ProcessKind.writer,
                IMutator _ => ProcessKind.mutator,
                IScope _ => ProcessKind.scope,
                IEvaluable _ => ProcessKind.producer,
                IExecutable _ => ProcessKind.job,
                _ => ProcessKind.unknown,
            };
        }
    }
}