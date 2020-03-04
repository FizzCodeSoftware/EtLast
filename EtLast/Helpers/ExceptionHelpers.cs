namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public static class ExceptionHelpers
    {
        public static string FormatExceptionWithDetails(this Exception exception, bool includeTrace = true)
        {
            var lvl = 0;
            var msg = "EXCEPTION: ";

            var cex = exception;
            while (cex != null)
            {
                if (lvl > 0)
                    msg += "\nINNER EXCEPTION: ";

                msg += cex.GetType().GetFriendlyTypeName() + ": " + cex.Message;

                if (cex.Data?["Process"] is string storedProcess)
                {
                    msg += "\n\tPROCESS: " + storedProcess;
                }

                if (includeTrace)
                {
                    var trace = cex.Data?["Trace"] is string TraceCaller
                        ? TraceCaller
                        : EtlException.GetTraceFromStackFrames(new StackTrace(cex, true).GetFrames());

                    msg += "\n\tTRACE:\n\t\t" + trace.Replace("\n", "\n\t\t", StringComparison.Ordinal);
                }

                if (cex.Data?.Count > 0)
                {
                    var first = true;
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (k == "Process")
                            continue;

                        if (k == "CallChain")
                            continue;

                        if (k == "OpsMessage")
                            continue;

                        if (k == "Trace")
                            continue;

                        if (k == "Row")
                            continue;

                        if (k.Contains("Row", StringComparison.InvariantCultureIgnoreCase) && cex.Data[key] is string rowStr && rowStr.StartsWith("uid", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (first)
                        {
                            msg += "\n\tDATA: ";
                            first = false;
                        }
                        else
                        {
                            msg += ", ";
                        }

                        var value = cex.Data[key];
                        msg += "[" + k + "] = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                if (cex.Data?["Row"] is string storedRow)
                {
                    msg += "\n\tROW: " + storedRow;
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

                cex = cex.InnerException;
                lvl++;
            }

            return msg;
        }
    }
}