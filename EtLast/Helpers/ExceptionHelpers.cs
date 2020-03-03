namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public static class ExceptionHelpers
    {
        public static string FormatExceptionWithDetails(this Exception exception, bool includeCaller = true)
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

                if (includeCaller)
                {
                    if (cex.Data?["Caller"] is string storedCaller)
                    {
                        msg += "\n\tCALLER: " + storedCaller;
                    }
                    else
                    {
                        var frame = new StackTrace(cex, true).GetFrames()[0];
                        if (frame != null)
                        {
                            msg += "\n\tCALLER: " + EtlException.FrameToString(frame);
                        }
                    }
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

                        if (k == "Caller")
                            continue;

                        if (k == "Row")
                            continue;

                        if (first)
                        {
                            msg += "\n\tDATA: ";
                            first = false;
                        }
                        else
                            msg += ", ";

                        var value = cex.Data[key];
                        msg += "[" + k + "] = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                if (cex.Data?["Row"] is string storedRow)
                {
                    msg += "\n\tROW: " + storedRow;
                }

                cex = cex.InnerException;
                lvl++;
            }

            return msg;
        }
    }
}