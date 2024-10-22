﻿namespace FizzCode.EtLast;

public sealed class InMemoryRowCache : AbstractRowSource
{
    private bool _firstEvaluationFinished;
    private List<IReadOnlySlimRow> _cache;

    /// <summary>
    /// The process evaluates and yields the rows from the input process.
    /// </summary>
    public required ISequence InputProcess { get; init; }

    public int CurrentRowCount => _cache?.Count ?? 0;

    protected override void ValidateImpl()
    {
        if (InputProcess == null)
            throw new ProcessParameterNullException(this, nameof(InputProcess));
    }

    protected override IEnumerable<IRow> Produce()
    {
        if (_cache != null)
        {
            if (!_firstEvaluationFinished)
            {
                throw new ProcessExecutionException(this, "the memory cache is not built yet before the second evaluation");
            }

            foreach (var row in _cache)
            {
                if (FlowState.IsTerminating)
                    yield break;

                var newRow = Context.CreateRow(this, row);
                yield return newRow;
            }

            yield break;
        }
        else
        {
            _cache = [];
            var inputRows = InputProcess.TakeRowsAndReleaseOwnership(this, FlowState);
            foreach (var row in inputRows)
            {
                if (FlowState.IsTerminating)
                    break;

                if (IgnoreRowsWithError && row.HasError())
                    continue;

                _cache.Add(row);

                var newRow = Context.CreateRow(this, row);
                yield return newRow;
            }

            _firstEvaluationFinished = true;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class InMemoryRowCacheFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromInMemoryRowCache(this IFluentSequenceBuilder builder, InMemoryRowCache cache)
    {
        return builder.ReadFrom(cache);
    }

    public static InMemoryRowCache BuildToInMemoryRowCache(this IFluentSequenceMutatorBuilder builder, string name = null)
    {
        return new InMemoryRowCache()
        {
            Name = name,
            InputProcess = builder.ProcessBuilder.Result,
        };
    }
}
