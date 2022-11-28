namespace FizzCode.EtLast;

public class Pipe
{
    public IEtlContext Context { get; }

    public List<Exception> Exceptions { get; } = new List<Exception>();
    public bool Failed => Exceptions.Count > 0;
    public bool IsTerminating => Context.IsTerminating || Failed;

    public Pipe(IEtlContext context)
    {
        Context = context;
    }

    public void AddException(IProcess process, Exception ex)
    {
        if (ex is AggregateException aex)
        {
            foreach (var aexIn in aex.InnerExceptions)
            {
                AddException(process, aexIn);
            }

            return;
        }

        if (ex is OperationCanceledException)
            return;

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

    public string ToLogString()
    {
        if (Failed)
            return "failed";

        return "completed";
    }

    public void TakeExceptions(Pipe from)
    {
        if (from.Exceptions.Count > 0)
            Exceptions.AddRange(from.Exceptions);
    }
}