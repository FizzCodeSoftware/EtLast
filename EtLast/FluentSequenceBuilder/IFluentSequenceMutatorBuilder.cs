namespace FizzCode.EtLast;

public interface IFluentSequenceMutatorBuilder : ISequenceBuilder
{
    IFluentSequenceBuilder ProcessBuilder { get; }
    IFluentSequenceMutatorBuilder AddMutator(IMutator mutator);
    IFluentSequenceMutatorBuilder AddMutators(IEnumerable<IMutator> mutators);

    IFluentSequenceMutatorBuilder IfTrue(bool flag, Action<IFluentSequenceMutatorBuilder> builder);
    IFluentSequenceMutatorBuilder If(RowTestDelegate rowTester, Action<IFluentSequenceMutatorBuilder> builder);
    IFluentSequenceMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentSequenceMutatorBuilder> builder);
}