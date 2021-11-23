namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMutator : IProducer, IEnumerable<IMutator>
    {
        public IProducer InputProcess { get; set; }
        public RowTestDelegate If { get; set; }
        public RowTagTestDelegate TagFilter { get; set; }
    }
}