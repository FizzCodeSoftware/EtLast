namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMerger : AbstractSequence, IMerger
{
    public List<ISequence> SequenceList { get; set; }

    public override bool ConsumerShouldNotBuffer => SequenceList?.Any(x => x is ISequence s && s.ConsumerShouldNotBuffer) == true;

    protected AbstractMerger(IEtlContext context)
        : base(context)
    {
    }
}