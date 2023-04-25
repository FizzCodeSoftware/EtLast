namespace FizzCode.EtLast;

public static class ExceptionHelpers
{
    public static string FormatExceptionWithDetails(this Exception exception, bool includeTrace = true)
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

                    if (cex.Data?["ProcessTopic"] is string processTopic)
                        msg += ", topic: " + processTopic;

                    if (cex.Data?["ProcessKind"] is string processKind)
                        msg += ", kind: " + processKind;
                }

                if (cex.Data?.Count > 0)
                {
                    var first = true;
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k is "ProcessName" or "ProcessKind" or "ProcessTopic" or "ProcessType" or "ProcessTypeAssembly" or "CallChain" or "OpsMessage" or "Trace" or "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase) && cex.Data[key] is string rowStr && rowStr.StartsWith("uid", StringComparison.InvariantCultureIgnoreCase))
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
                        msg += "\n\t\t[" + k + "] = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                if (cex.Data?["Row"] is string storedRow)
                {
                    msg += "\n\tROW: " + storedRow.Replace("\n", "\n\t\t", StringComparison.InvariantCultureIgnoreCase);
                }

                if (cex.Data?.Count > 0)
                {
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k == "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase) && cex.Data[key] is string rowStr && rowStr.StartsWith("uid", StringComparison.InvariantCultureIgnoreCase))
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
                        trace = EtlException.GetTraceFromStackFrames(new StackTrace(cex, true).GetFrames());

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
