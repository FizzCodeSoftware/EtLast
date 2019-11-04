namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractMergeProcess : IMergeProcess
    {
        public string Name { get; set; }
        public IEtlContext Context { get; }
        public IRowSetMerger Merger { get; }
        public IExecutionBlock Caller { get; protected set; }
        public bool ConsumerShouldNotBuffer => false;

        protected List<IProcess> InputProcesses { get; } = new List<IProcess>();

        protected AbstractMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Merger = merger;
            Name = name ?? TypeHelpers.GetFriendlyTypeName(GetType());
        }

        public abstract IEnumerable<IRow> Evaluate(IExecutionBlock caller = null);

        public void AddInput(IProcess inputProcess)
        {
            InputProcesses.Add(inputProcess);
        }
    }
}