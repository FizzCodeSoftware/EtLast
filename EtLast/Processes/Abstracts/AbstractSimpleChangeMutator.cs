namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSimpleChangeMutator : AbstractMutator
{
    protected List<KeyValuePair<string, object>> Changes;

    protected AbstractSimpleChangeMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        base.StartMutator();
        Changes = [];
    }

    protected override void CloseMutator()
    {
        if (Changes != null)
        {
            Changes.Clear();
            Changes = null;
        }
    }
}
