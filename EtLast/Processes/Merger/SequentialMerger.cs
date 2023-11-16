namespace FizzCode.EtLast;

public sealed class SequentialMerger(IEtlContext context) : AbstractMerger(context)
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

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class SequentialMergerFluent
{
    public static IFluentSequenceMutatorBuilder SequentialMerge(this IFluentSequenceBuilder builder, IEtlContext context, string name, Action<SequentialMergerBuilder> merger)
    {
        var subBuilder = new SequentialMergerBuilder(context, name);
        merger.Invoke(subBuilder);
        return builder.ReadFrom(subBuilder.Merger);
    }
}

public class SequentialMergerBuilder
{
    public SequentialMerger Merger { get; }

    internal SequentialMergerBuilder(IEtlContext context, string name)
    {
        Merger = new SequentialMerger(context)
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