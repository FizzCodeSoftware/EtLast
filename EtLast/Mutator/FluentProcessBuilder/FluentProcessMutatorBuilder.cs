namespace FizzCode.EtLast;

internal sealed class FluentProcessMutatorBuilder : IFluentProcessMutatorBuilder
{
    public IFluentProcessBuilder ProcessBuilder { get; }
    internal RowTestDelegate AutomaticallySetRowFilter { get; set; }
    internal RowTagTestDelegate AutomaticallySetRowTagFilter { get; set; }

    internal FluentProcessMutatorBuilder(IFluentProcessBuilder parent)
    {
        ProcessBuilder = parent;
    }

    public IFluentProcessMutatorBuilder AddMutator(IMutator mutator)
    {
        if (AutomaticallySetRowFilter != null)
            mutator.RowFilter = AutomaticallySetRowFilter;

        if (AutomaticallySetRowTagFilter != null)
            mutator.RowTagFilter = AutomaticallySetRowTagFilter;

        mutator.InputProcess = ProcessBuilder.Result;
        ProcessBuilder.Result = mutator;
        return this;
    }

    public IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators)
    {
        foreach (var mutator in mutators)
        {
            if (AutomaticallySetRowTagFilter != null)
            {
                mutator.RowTagFilter = AutomaticallySetRowTagFilter;
            }

            mutator.InputProcess = ProcessBuilder.Result;
            ProcessBuilder.Result = mutator;
        }

        return this;
    }

    public IFluentProcessMutatorBuilder If(RowTestDelegate rowTester, Action<IFluentProcessMutatorBuilder> builder)
    {
        var tempBuilder = new FluentProcessMutatorBuilder(ProcessBuilder)
        {
            AutomaticallySetRowFilter = rowTester,
        };

        builder.Invoke(tempBuilder);

        return this;
    }

    public IFluentProcessMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentProcessMutatorBuilder> builder)
    {
        var tempBuilder = new FluentProcessMutatorBuilder(ProcessBuilder)
        {
            AutomaticallySetRowTagFilter = tagTester,
        };

        builder.Invoke(tempBuilder);

        return this;
    }

    public IProducer Build()
    {
        return ProcessBuilder.Build();
    }
}
