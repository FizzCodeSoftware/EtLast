namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMergerProcess : IEvaluable
    {
        List<IEvaluable> ProcessList { get; set; }
    }
}