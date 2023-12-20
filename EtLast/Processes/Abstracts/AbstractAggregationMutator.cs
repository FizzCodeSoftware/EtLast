namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractAggregationMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    /// <summary>
    /// Null is allowed, which means no fix columns will be copied to the output.
    /// </summary>
    public required Dictionary<string, string> FixColumns { get; init; }

    /// <summary>
    /// Null is allowed, which means no partitioning will happen on the source.
    /// </summary>
    public required Func<IRow, string> KeyGenerator { get; init; }

    protected AbstractAggregationMutator()
    {
    }

    public IEnumerator<IMutator> GetEnumerator()
    {
        yield return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        yield return this;
    }
}
