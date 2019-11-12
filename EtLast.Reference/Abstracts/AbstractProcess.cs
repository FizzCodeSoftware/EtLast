using System.Diagnostics;

namespace FizzCode.EtLast
{
    public abstract class AbstractProcess : IProcess
    {
        public ProcessTestDelegate If { get; set; }
        public IEtlContext Context { get; }
        public IProcess Caller { get; protected set; }
        public string Name { get; }
        public Stopwatch LastInvocation { get; protected set; }

        protected AbstractProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name ?? TypeHelpers.GetFriendlyTypeName(GetType());
        }

        public abstract void Validate();
    }
}