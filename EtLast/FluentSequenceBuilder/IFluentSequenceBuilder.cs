namespace FizzCode.EtLast;

public interface IFluentSequenceBuilder : ISequenceBuilder
{
    ISequence Result { get; set; }
    IFluentSequenceMutatorBuilder ReadFrom(ISequence process);
}
