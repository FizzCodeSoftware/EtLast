namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EtlExceptionExtensions
{
    public static string FormatWithEtlDetails(this Exception exception, bool includeTrace = true, int skipStackFrames = 0)
    {
        try
        {
            var lvl = 0;
            var msg = "EXCEPTION: ";

            var cex = exception;
            while (cex != null)
            {
                if (lvl > 0)
                    msg += "\nINNER EXCEPTION: ";

                msg += cex.GetType().GetFriendlyTypeName() + ": " + cex.Message;

                if (cex.Data?["ProcessType"] is string processType)
                {
                    msg += "\n\tPROCESS: ";

                    if (cex.Data?["ProcessTypeAssembly"] is string processTypeAssembly)
                        msg += "(" + processTypeAssembly + ") ";

                    msg += processType;

                    if (cex.Data?["ProcessName"] is string processName && processName != processType)
                        msg += " (\"" + processName + "\")";

                    if (cex.Data?["ProcessKind"] is string processKind)
                        msg += ", kind: " + processKind;
                }

                if (cex.Data?.Count > 0)
                {
                    var first = true;
                    var maxKeyLength = 0;
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k is "ProcessName" or "ProcessKind" or "ProcessType" or "ProcessTypeAssembly" or "CallChain" or "OpsMessage" or "Trace" or "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase) && cex.Data[key] is string rowStr && rowStr.StartsWith("id", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        var l = k.Length;
                        if (l > maxKeyLength)
                            maxKeyLength = l;
                    }

                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k is "ProcessName" or "ProcessKind" or "ProcessType" or "ProcessTypeAssembly" or "CallChain" or "OpsMessage" or "Trace" or "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (first)
                        {
                            msg += "\n\tDATA:";
                            first = false;
                        }
                        else
                        {
                            msg += ", ";
                        }

                        var value = cex.Data[key];
                        msg += "\n\t\t" + ("[" + k + "]").PadRight(maxKeyLength + 3) + " = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                if (cex.Data?["Row"] is string storedRow)
                {
                    msg += "\n\tROW:\n\t\t" + storedRow.Replace("\n", "\n\t\t", StringComparison.InvariantCultureIgnoreCase);
                }

                if (cex.Data?.Count > 0)
                {
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k == "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase) && cex.Data[key] is string rowStr)
                        {
                            msg += "\n\t" + k.ToUpperInvariant() + ": " + rowStr;
                        }
                    }
                }

                if (cex.Data?["CallChain"] is string callChain)
                    msg += "\n\tCALL CHAIN:\n\t\t" + callChain.Replace("\n", "\n\t\t", StringComparison.Ordinal);

                if (includeTrace)
                {
                    if (cex.Data?["Trace"] is not string trace)
                        trace = ExceptionExtensions.GetTraceFromStackFrames(new StackTrace(cex, true).GetFrames(), lvl == 0 ? 1 + skipStackFrames : 0);

                    if (trace != null)
                    {
                        msg += "\n\tTRACE:\n\t\t" + trace.Replace("\n", "\n\t\t", StringComparison.Ordinal);
                    }
                }

                cex = cex.InnerException;
                lvl++;
            }

            return msg;
        }
        catch (Exception)
        {
            return exception.ToString();
        }
    }
}