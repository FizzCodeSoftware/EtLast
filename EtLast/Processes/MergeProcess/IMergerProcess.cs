namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMergerProcess : IEvaluable
    {
        IRowSetMerger Merger { get; }
        List<IEvaluable> ProcessList { get; set; }
    }
}