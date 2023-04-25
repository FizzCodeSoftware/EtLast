namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMerger : AbstractSequence, IMerger
{
    public required List<ISequence> SequenceList { get; init; }

    protected AbstractMerger(IEtlContext context)
        : base(context)
    {
    }
}