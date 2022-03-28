namespace FizzCode.EtLast;

public sealed class MutatorList : List<IEnumerable<IMutator>>
{
    public MutatorList()
    {
    }

    public MutatorList(IEnumerable<IEnumerable<IMutator>> collection)
        : base(collection)
    {
    }
}
