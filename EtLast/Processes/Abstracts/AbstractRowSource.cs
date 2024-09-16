namespace FizzCode.EtLast;

/// <summary>
/// Row sources create rows - they may create or generate, read from different sources, copy from existing rows.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractRowSource : AbstractSequence, IRowSource
{
    /// <summary>
    /// Default false.
    /// </summary>
    public bool IgnoreRowsWithError { get; init; }

    /// <summary>
    /// Default true.
    /// </summary>
    public bool IgnoreNullOrEmptyRows { get; init; } = true;

    protected bool AutomaticallyEvaluateAndYieldInputProcessRows { get; init; } = true;

    protected AbstractRowSource()
    {
    }

    protected sealed override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var resultCount = 0;

        netTimeStopwatch.Stop();
        var enumerator = Produce().GetEnumerator();
        netTimeStopwatch.Start();

        while (!FlowState.IsTerminating)
        {
            IRow row = null;
            try
            {
                if (!enumerator.MoveNext())
                    break;

                row = enumerator.Current;

                if (row.Tag is not HeartBeatTag && !ProcessRowBeforeYield(row))
                    continue;
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, ex);
            }

            if (row != null)
            {
                resultCount++;
                netTimeStopwatch.Stop();
                yield return row;
                netTimeStopwatch.Start();
            }
        }

        Context.Log(LogSeverity.Debug, this, "produced {RowCount} rows",
            resultCount);
    }

    protected abstract IEnumerable<IRow> Produce();

    private bool ProcessRowBeforeYield(IRow row)
    {
        if (IgnoreRowsWithError && row.HasError())
            return false;

        if (IgnoreNullOrEmptyRows && row.IsNullOrEmpty())
            return false;

        return true;
    }
}
