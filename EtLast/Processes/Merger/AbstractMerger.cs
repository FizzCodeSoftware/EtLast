namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractMerger : AbstractEvaluable, IMerger
    {
        public List<IEvaluable> ProcessList { get; set; }

        public override bool ConsumerShouldNotBuffer => ProcessList?.Any(x => x is IEvaluable p && p.ConsumerShouldNotBuffer) == true;

        protected AbstractMerger(ITopic topic, string name)
            : base(topic, name)
        {
        }
    }
}