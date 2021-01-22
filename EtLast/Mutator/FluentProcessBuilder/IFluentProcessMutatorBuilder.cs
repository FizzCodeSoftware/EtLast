namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IFluentProcessMutatorBuilder : IProcessBuilder
    {
        IFluentProcessBuilder ProcessBuilder { get; }
        IFluentProcessMutatorBuilder AddMutator(IMutator mutator);
        IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators);
    }
}