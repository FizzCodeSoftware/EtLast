namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ProcessExecutionException : EtlException
{
    public ProcessExecutionException(IProcess process, Exception innerException)
        : this(process, "error in a process", innerException)
    {
    }

    public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, Exception innerException)
        : this(process, "error in a process", innerException)
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public ProcessExecutionException(IProcess process, string message)
        : base(process, message)
    {
    }

    public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, string message)
        : base(process, message)
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public ProcessExecutionException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }

    public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, string message, Exception innerException)
        : base(process, message, innerException)
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public static EtlException Wrap(IProcess process, IReadOnlySlimRow row, Exception ex)
    {
        if (ex is EtlException eex)
        {
            var str = row.ToDebugString(true);
            if ((eex.Data["Row"] is string rowString) && string.Equals(rowString, str, StringComparison.Ordinal))
            {
                return eex;
            }
            else
            {
                eex.Data["Row"] = str;
                return eex;
            }
        }

        return new ProcessExecutionException(process, row, ex);
    }
}