namespace FizzCode.EtLast;

public interface IMutator : ISequence, IEnumerable<IMutator>
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }
}
