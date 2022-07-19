namespace FizzCode.EtLast;

internal sealed class FluentSequenceBuilder : IFluentSequenceBuilder
{
    internal FluentSequenceBuilder()
    {
    }

    public ISequence Result { get; set; }

    public ISequence Build()
    {
        return Result;
    }

    public IFluentSequenceMutatorBuilder ReadFrom(ISequence process)
    {
        Result = process;
        return new FluentSequenceMutatorBuilder(this);
    }
}