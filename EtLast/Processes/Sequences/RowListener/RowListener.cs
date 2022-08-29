using System.Collections.Concurrent;

namespace FizzCode.EtLast.Processes.Producers.RowListener;

public sealed class RowListener : AbstractRowSource, IRowListener
{
    public Action<IRowListener> Worker { get; set; }

    /// <summary>
    /// Default value is 1000.
    /// </summary>
    public int HeartBeatMilliseconds { get; set; } = 1000;

    private ConcurrentQueue<IReadOnlySlimRow> _queue;

    public RowListener(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Worker == null)
            throw new ProcessParameterNullException(this, nameof(Worker));
    }

    protected override IEnumerable<IRow> Produce()
    {
        var thread = new Thread(() => Worker(this));

        _queue = new ConcurrentQueue<IReadOnlySlimRow>();
        Stopwatch hbTimer = null;
        var hbCount = 0;
        var rowCount = 0;

        thread.Start();
        while (!Context.IsTerminating)
        {
            var hbElapsed = hbTimer.ElapsedMilliseconds;
            if (hbElapsed >= HeartBeatMilliseconds)
            {
                var hbRow = Context.CreateRow(this);
                hbRow["Index"] = hbCount;
                hbRow["RowCount"] = rowCount;
                hbRow["Now"] = DateTimeOffset.Now;
                if (hbCount > 0)
                    hbRow["Elapsed"] = hbElapsed;

                yield return hbRow;
                hbCount++;
            }

            if (_queue.TryDequeue(out var row))
            {
                yield return Context.CreateRow(this, row);
                rowCount++;
                continue;
            }
            else if (!thread.IsAlive)
            {
                break;
            }

            Thread.Sleep(1);
        }

        thread.Join();

        _queue.Clear();
        _queue = null;
    }

    public void AddRow(IReadOnlySlimRow row)
    {
        _queue?.Enqueue(row);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RowListenerFluent
{
    public static IFluentSequenceMutatorBuilder Listen(this IFluentSequenceBuilder builder, RowListener listener)
    {
        return builder.ReadFrom(listener);
    }
}