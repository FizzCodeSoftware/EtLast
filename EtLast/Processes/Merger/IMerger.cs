namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMerger : IProducer
    {
        List<IProducer> ProcessList { get; set; }
    }
}