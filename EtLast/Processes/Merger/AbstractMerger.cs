namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMerger : AbstractSequence, IMerger
{
    public required List<ISequence> SequenceList { get; init; }

    protected AbstractMerger()
    {
    }
}