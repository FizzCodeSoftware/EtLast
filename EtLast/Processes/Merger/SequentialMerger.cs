namespace FizzCode.EtLast;

public sealed class SequentialMerger : AbstractMerger
{
    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        foreach (var sequence in SequenceList)
        {
            if (FlowState.IsTerminating)
                break;

            var rows = sequence.TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (FlowState.IsTerminating)
                    break;

                yield return row;
            }
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SequentialMergerFluent
{
    public static IFluentSequenceMutatorBuilder SequentialMerge(this IFluentSequenceBuilder builder, string name, Action<SequentialMergerBuilder> merger)
    {
        var subBuilder = new SequentialMergerBuilder(name);
        merger.Invoke(subBuilder);
        return builder.ReadFrom(subBuilder.Merger);
    }
}

public class SequentialMergerBuilder
{
    public SequentialMerger Merger { get; }

    internal SequentialMergerBuilder(string name)
    {
        Merger = new SequentialMerger()
        {
            Name = name,
            SequenceList = [],
        };
    }

    public SequentialMergerBuilder Add(ISequence sequence)
    {
        Merger.SequenceList.Add(sequence);
        return this;
    }
}