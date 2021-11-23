namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMutator : IProducer, IEnumerable<IMutator>
    {
        public IProducer InputProcess { get; set; }
        public RowTestDelegate RowFilter { get; set; }
        public RowTagTestDelegate RowTagFilter { get; set; }
    }
}