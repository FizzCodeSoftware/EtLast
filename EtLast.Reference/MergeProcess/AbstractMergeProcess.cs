namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractMergeProcess : AbstractEvaluableProcess, IMergerProcess
    {
        public IRowSetMerger Merger { get; }
        public List<IEvaluable> ProcessList { get; set; }

        public override bool ConsumerShouldNotBuffer => ProcessList?.Any(x => x is IEvaluable p && p.ConsumerShouldNotBuffer) == true;

        protected AbstractMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
            : base(context, name)
        {
            Merger = merger;
        }
    }
}