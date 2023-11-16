namespace FizzCode.EtLast;

public class FlowState(IEtlContext context) : IReadOnlyFlowState
{
    public IEtlContext Context { get; } = context;

    public List<Exception> Exceptions { get; } = [];
    public bool Failed => Exceptions.Count > 0;
    public bool IsTerminating => Context.IsTerminating || Failed;

    public void AddException(IProcess process, Exception ex)
    {
        if (ex is OperationCanceledException)
            return;

        if (Exceptions.Contains(ex))
            return;

        if (ex is AggregateException aex)
        {
            foreach (var aexIn in aex.InnerExceptions)
            {
                AddException(process, aexIn); // recursion
            }

            return;
        }

        if (ex is not EtlException)
            ex = new ProcessExecutionException(process, ex);

        foreach (var listener in Context.Listeners)
        {
            listener.OnException(process, ex);
        }

        Exceptions.Add(ex);
    }

    public void AddException(IProcess process, Exception ex, IReadOnlySlimRow row)
    {
        if (ex is OperationCanceledException)
            return;

        if (Exceptions.Contains(ex))
            return;

        if (ex is EtlException eex)
        {
            var str = row.ToDebugString(true);
            if ((eex.Data["Row"] is not string rowString) || !string.Equals(rowString, str, StringComparison.Ordinal))
            {
                eex.Data["Row"] = str;
            }
        }
        else
        {
            ex = new ProcessExecutionException(process, row, ex);
        }

        foreach (var listener in Context.Listeners)
        {
            listener.OnException(process, ex);
        }

        Exceptions.Add(ex);
    }

    public string StatusToLogString()
    {
        if (Failed)
            return "failed";

        return "completed";
    }

    public void TakeExceptions(FlowState from)
    {
        if (from.Exceptions.Count > 0)
            Exceptions.AddRange(from.Exceptions);
    }
}