namespace FizzCode.EtLast
{
    public interface IReadOnlyRow : IReadOnlySlimRow
    {
        IEtlContext Context { get; }
        int Uid { get; }

        IProcess CreatorProcess { get; }
        IProcess CurrentProcess { get; }

        bool HasStaging { get; }
    }
}