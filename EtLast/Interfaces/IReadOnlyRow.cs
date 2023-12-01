namespace FizzCode.EtLast;

public interface IReadOnlyRow : IReadOnlySlimRow
{
    long Id { get; }

    IProcess Owner { get; }
}
