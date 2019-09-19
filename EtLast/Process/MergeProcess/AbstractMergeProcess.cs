namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractMergeProcess : IMergeProcess
    {
        public string Name { get; set; }
        public IEtlContext Context { get; }
        public IRowSetMerger Merger { get; }
        public ICaller Caller { get; protected set; }
        public bool ConsumerShouldNotBuffer => false;

        protected List<IProcess> InputProcesses { get; } = new List<IProcess>();

        protected AbstractMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Merger = merger;
            Name = name ?? GetType().Name;
        }

        public abstract IEnumerable<IRow> Evaluate(ICaller caller = null);

        public void AddInput(IProcess inputProcess)
        {
            InputProcesses.Add(inputProcess);
        }
    }
}