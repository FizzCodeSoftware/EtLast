namespace FizzCode.EtLast;

public interface IReadOnlyRow : IReadOnlySlimRow
{
    public long Id { get; }
    public IProcess Owner { get; }
    public long GetRowChecksumForSpecificColumns(string[] columns);
    public long GetRowChecksumForAllColumns(string[] exceptColumns);
}
