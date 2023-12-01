namespace FizzCode.EtLast;

public interface IRow : IReadOnlyRow, ISlimRow
{
    new IProcess Owner { get; }
    void SetOwner(IProcess currentProcess);

    void MergeWith(IEnumerable<KeyValuePair<string, object>> values);
}