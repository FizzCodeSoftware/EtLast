namespace FizzCode.EtLast;

internal sealed class FluentSequenceMutatorBuilder : IFluentSequenceMutatorBuilder
{
    public required IFluentSequenceBuilder ProcessBuilder { get; init; }
    internal required RowTestDelegate AutomaticallySetRowFilter { get; init; }
    internal required RowTagTestDelegate AutomaticallySetRowTagFilter { get; init; }

    public IFluentSequenceMutatorBuilder AddMutator(IMutator mutator)
    {
        if (AutomaticallySetRowFilter != null)
            mutator.RowFilter = AutomaticallySetRowFilter;

        if (AutomaticallySetRowTagFilter != null)
            mutator.RowTagFilter = AutomaticallySetRowTagFilter;

        mutator.Input = ProcessBuilder.Result;
        ProcessBuilder.Result = mutator;
        return this;
    }

    public IFluentSequenceMutatorBuilder AddMutators(IEnumerable<IMutator> mutators)
    {
        foreach (var mutator in mutators)
        {
            if (AutomaticallySetRowTagFilter != null)
            {
                mutator.RowTagFilter = AutomaticallySetRowTagFilter;
            }

            mutator.Input = ProcessBuilder.Result;
            ProcessBuilder.Result = mutator;
        }

        return this;
    }

    public IFluentSequenceMutatorBuilder If(RowTestDelegate rowTester, Action<IFluentSequenceMutatorBuilder> builder)
    {
        builder.Invoke(new FluentSequenceMutatorBuilder()
        {
            ProcessBuilder = ProcessBuilder,
            AutomaticallySetRowFilter = rowTester,
            AutomaticallySetRowTagFilter = null,
        });

        return this;
    }

    public IFluentSequenceMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentSequenceMutatorBuilder> builder)
    {
        builder.Invoke(new FluentSequenceMutatorBuilder()
        {
            ProcessBuilder = ProcessBuilder,
            AutomaticallySetRowFilter = null,
            AutomaticallySetRowTagFilter = tagTester,
        });

        return this;
    }

    public ISequence Build()
    {
        return ProcessBuilder.Result;
    }
}
