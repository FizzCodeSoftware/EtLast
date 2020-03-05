namespace FizzCode.EtLast
{
    public interface IReadOnlyEtlRow : IReadOnlyRow
    {
        IEtlContext Context { get; }
        int Uid { get; }

        IProcess CreatorProcess { get; }
        IProcess CurrentProcess { get; }

        bool HasStaging { get; }
    }
}