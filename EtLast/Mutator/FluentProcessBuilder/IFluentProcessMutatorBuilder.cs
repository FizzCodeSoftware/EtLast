namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IFluentProcessMutatorBuilder : IProcessBuilder
    {
        IFluentProcessBuilder ProcessBuilder { get; }
        IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators);
    }
}