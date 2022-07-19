namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMerger : AbstractSequence, IMerger
{
    public List<ISequence> SequenceList { get; set; }

    protected AbstractMerger(IEtlContext context)
        : base(context)
    {
    }
}