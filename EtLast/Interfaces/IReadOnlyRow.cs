namespace FizzCode.EtLast;

public interface IReadOnlyRow : IReadOnlySlimRow
{
    IEtlContext Context { get; }
    long Id { get; }

    IProcess CreatorProcess { get; }
    IProcess CurrentProcess { get; }
}
