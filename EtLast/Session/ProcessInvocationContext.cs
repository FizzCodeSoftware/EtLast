namespace FizzCode.EtLast;

public class ProcessInvocationContext
{
    public IEtlContext Context { get; }

    public List<Exception> Exceptions { get; } = new List<Exception>();
    public bool Failed => Exceptions.Count > 0;
    public bool IsTerminating => Context.CancellationToken.IsCancellationRequested || Failed;

    public ProcessInvocationContext(IEtlContext context)
    {
        Context = context;
    }

    public void AddException(IProcess process, Exception ex)
    {
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

        return "success";
    }

    public void TakeExceptions(ProcessInvocationContext otherInvocationContext)
    {
        if (otherInvocationContext.Exceptions.Count > 0)
            Exceptions.AddRange(otherInvocationContext.Exceptions);
    }
}