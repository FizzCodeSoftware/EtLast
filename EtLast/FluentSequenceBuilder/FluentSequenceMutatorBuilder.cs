namespace FizzCode.EtLast;

internal sealed class FluentSequenceMutatorBuilder : IFluentSequenceMutatorBuilder
{
    public IFluentSequenceBuilder ProcessBuilder { get; }
    internal RowTestDelegate AutomaticallySetRowFilter { get; set; }
    internal RowTagTestDelegate AutomaticallySetRowTagFilter { get; set; }

    internal FluentSequenceMutatorBuilder(IFluentSequenceBuilder parent)
    {
        ProcessBuilder = parent;
    }

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
        var tempBuilder = new FluentSequenceMutatorBuilder(ProcessBuilder)
        {
            AutomaticallySetRowFilter = rowTester,
        };

        builder.Invoke(tempBuilder);

        return this;
    }

    public IFluentSequenceMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentSequenceMutatorBuilder> builder)
    {
        var tempBuilder = new FluentSequenceMutatorBuilder(ProcessBuilder)
        {
            AutomaticallySetRowTagFilter = tagTester,
        };

        builder.Invoke(tempBuilder);

        return this;
    }

    public ISequence Build()
    {
        return ProcessBuilder.Build();
    }
}
