namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMerger : AbstractEvaluable, IMerger
{
    public List<IProducer> ProcessList { get; set; }

    public override bool ConsumerShouldNotBuffer => ProcessList?.Any(x => x is IProducer p && p.ConsumerShouldNotBuffer) == true;

    protected AbstractMerger(IEtlContext context)
        : base(context)
    {
    }
}
