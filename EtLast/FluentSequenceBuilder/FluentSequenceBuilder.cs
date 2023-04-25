namespace FizzCode.EtLast;

internal sealed class FluentSequenceBuilder : IFluentSequenceBuilder
{
    public ISequence Result { get; set; }

    internal FluentSequenceBuilder()
    {
    }

    public ISequence Build()
    {
        return Result;
    }

    public IFluentSequenceMutatorBuilder ReadFrom(ISequence process)
    {
        Result = process;
        return new FluentSequenceMutatorBuilder()
        {
            ProcessBuilder = this,
            AutomaticallySetRowFilter = null,
            AutomaticallySetRowTagFilter = null,
        };
    }
}