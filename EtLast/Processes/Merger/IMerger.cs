namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMerger : IEvaluable
    {
        List<IEvaluable> ProcessList { get; set; }
    }
}