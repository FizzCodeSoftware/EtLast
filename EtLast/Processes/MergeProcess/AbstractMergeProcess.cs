namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractMergeProcess : AbstractEvaluableProcess, IMergerProcess
    {
        public List<IEvaluable> ProcessList { get; set; }

        public override bool ConsumerShouldNotBuffer => ProcessList?.Any(x => x is IEvaluable p && p.ConsumerShouldNotBuffer) == true;

        protected AbstractMergeProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }
    }
}