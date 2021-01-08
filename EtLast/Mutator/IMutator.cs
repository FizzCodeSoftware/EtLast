namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMutator : IEvaluable, IEnumerable<IMutator>
    {
        public IEvaluable InputProcess { get; set; }
    }
}