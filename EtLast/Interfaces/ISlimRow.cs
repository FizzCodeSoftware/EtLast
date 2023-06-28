namespace FizzCode.EtLast;

public interface ISlimRow : IReadOnlySlimRow
{
    new object this[string column] { get; set; }
    new object Tag { get; set; }
    new bool KeepNulls { get; set; }
    void Clear();
}
