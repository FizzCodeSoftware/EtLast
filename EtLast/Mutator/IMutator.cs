namespace FizzCode.EtLast;

public interface IMutator : IProducer, IEnumerable<IMutator>
{
    public IProducer InputProcess { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }
}
