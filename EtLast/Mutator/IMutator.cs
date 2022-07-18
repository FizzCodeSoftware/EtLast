namespace FizzCode.EtLast;

public interface IMutator : IProducer, IEnumerable<IMutator>
{
    public IProducer Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }
}
