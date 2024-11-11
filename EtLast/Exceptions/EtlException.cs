namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class EtlException : Exception
{
    public static readonly string OpsMessageDataKey = "OpsMessage";

    public EtlException(IProcess process, string message)
      : base(message)
    {
        var trace = ExceptionExtensions.GetTraceFromStackFrames(new StackTrace(true).GetFrames(), 1);
        if (trace != null)
            Data["Trace"] = trace;

        Data["ProcessName"] = process.Name;

        Data["ProcessType"] = process.GetType().GetFriendlyTypeName();

        var assembly = process.GetType().Assembly?.GetName()?.Name;
        if (assembly != null)
            Data["ProcessTypeAssembly"] = assembly;

        Data["ProcessKind"] = process.Kind;

        var cc = GetCallChain(process);
        if (!string.IsNullOrEmpty(cc))
            Data["CallChain"] = cc;
    }

    public EtlException(IProcess process, string message, Exception innerException)
        : base(message + " (" + innerException.Message + ")", innerException)
    {
        if (innerException.Data["Trace"] != null)
        {
            Data["Trace"] = innerException.Data["Trace"];
            innerException.Data["Trace"] = null;
        }
        else
        {
            var trace = ExceptionExtensions.GetTraceFromStackFrames(new StackTrace(true).GetFrames(), 1);
            if (trace != null)
                Data["Trace"] = trace;
        }

        Data["ProcessName"] = process.Name;
        Data["ProcessType"] = process.GetType().GetFriendlyTypeName();

        var assembly = process.GetType().Assembly?.GetName()?.Name;
        if (assembly != null)
            Data["ProcessTypeAssembly"] = assembly;

        Data["ProcessKind"] = process.Kind;

        var cc = GetCallChain(process);
        if (!string.IsNullOrEmpty(cc))
            Data["CallChain"] = cc;
    }

    private static string GetCallChain(IProcess process)
    {
        var builder = new StringBuilder();

        var p = process.ExecutionInfo?.Caller as IProcess;
        while (p != null)
        {
            var assemblyName = p.GetType().Assembly?.GetName().Name;
            if (assemblyName != null)
            {
                builder.Append('(').Append(assemblyName).Append(") ");
            }

            var typeName = p.GetType().GetFriendlyTypeName();
            builder.Append(typeName);

            if (p.Name != typeName)
            {
                builder.Append(" (\"");
                builder.Append(p.Name);
                builder.Append("\")");
            }

            builder.Append(", kind: ");
            builder.AppendLine(p.Kind);

            p = p.ExecutionInfo?.Caller as IProcess;
        }

        return builder.ToString().Trim();
    }

    public void AddOpsMessage(string message)
    {
        Data.Add(OpsMessageDataKey, message);
    }
}
