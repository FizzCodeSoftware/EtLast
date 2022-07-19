namespace FizzCode.EtLast;

public interface IMerger : ISequence
{
    List<ISequence> SequenceList { get; set; }
}
