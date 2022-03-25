namespace FizzCode.EtLast;

using System.Collections.Generic;

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
