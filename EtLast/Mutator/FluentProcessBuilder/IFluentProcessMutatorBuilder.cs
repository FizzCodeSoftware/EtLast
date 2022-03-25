namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;

public interface IFluentProcessMutatorBuilder : IProcessBuilder
{
    IFluentProcessBuilder ProcessBuilder { get; }
    IFluentProcessMutatorBuilder AddMutator(IMutator mutator);
    IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators);

    IFluentProcessMutatorBuilder If(RowTestDelegate rowTester, Action<IFluentProcessMutatorBuilder> builder);
    IFluentProcessMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentProcessMutatorBuilder> builder);
}
