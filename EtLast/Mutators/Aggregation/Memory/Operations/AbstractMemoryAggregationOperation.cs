namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMemoryAggregationOperation : IMemoryAggregationOperation
{
    public AbstractMemoryAggregationMutator Process { get; private set; }

    public abstract void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator);

    public void SetProcess(AbstractMemoryAggregationMutator process)
    {
        Process = process;
    }
}
