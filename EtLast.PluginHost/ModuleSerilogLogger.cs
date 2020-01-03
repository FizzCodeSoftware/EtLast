namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Net.Http;
    using System.Text;
    using System.Text.Encodings.Web;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;

    public class ModuleSerilogLogger : IEtlPluginLogger, IDisposable
    {
        public ILogger Logger { get; set; }
        public ILogger OpsLogger { get; set; }
        public ModuleConfiguration ModuleConfiguration { get; set; }
        public Uri DiagnosticsUri { get; set; }

        private HttpClient _disgnosticsClient;
        private readonly object _customFileLock = new object();

        public void Log(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess caller, IBaseOperation operation, string text, params object[] args)
        {
            var ident = "";
            if (caller != null)
            {
                var p = caller;
                while (p.Caller != null)
                {
                    ident += "   ";
                    p = p.Caller;
                }
            }

            if (string.IsNullOrEmpty(ident))
                ident = " ";

            var values = new List<object>
            {
                ModuleConfiguration.ModuleName,
            };

            if (plugin != null)
                values.Add(plugin.Name);

            if (caller != null)
                values.Add(caller.Name);

            if (operation != null)
                values.Add(operation.Name);

            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? OpsLogger
                : Logger;

            logger.Write(
                (LogEventLevel)severity,
                "[{Module}"
                    + (plugin != null ? "/{Plugin}]" : "]")
                    + ident
                    + (caller != null ? "<{Caller}> " : "")
                    + (operation != null ? "({Operation}) " : "")
                    + text,
                values.ToArray());

            if (DiagnosticsUri != null)
            {
                SendDiagnostis(severity, forOps, plugin, caller, operation, text, args);
            }
        }

        public void LogException(IEtlPlugin plugin, ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(args.Exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, plugin, args.Caller, args.Operation, opsError);
            }

            var lvl = 0;
            var msg = "EXCEPTION: ";

            var cex = args.Exception;
            while (cex != null)
            {
                if (lvl > 0)
                    msg += "\nINNER EXCEPTION: ";

                msg += TypeHelpers.GetFriendlyTypeName(cex.GetType()) + ": " + cex.Message;

                if (cex.Data?.Count > 0)
                {
                    foreach (var key in cex.Data.Keys)
                    {
                        var k = key.ToString();
                        if (cex == args.Exception && k == "Process")
                            continue;

                        if (k == "CallChain")
                            continue;

                        if (k == "OpsMessage")
                            continue;

                        var value = cex.Data[key];
                        msg += ", " + k + " = " + (value != null ? value.ToString().Trim() : "NULL");
                    }
                }

                cex = cex.InnerException;
                lvl++;
            }

            Log(LogSeverity.Fatal, false, plugin, args.Caller, args.Operation, "{Message}", msg);
        }

        private void GetOpsMessagesRecursive(Exception ex, List<string> messages)
        {
            if (ex.Data.Contains(EtlException.OpsMessageDataKey))
            {
                var msg = ex.Data[EtlException.OpsMessageDataKey];
                if (msg != null)
                {
                    messages.Add(msg.ToString());
                }
            }

            if (ex.InnerException != null)
                GetOpsMessagesRecursive(ex.InnerException, messages);

            if (ex is AggregateException aex)
            {
                foreach (var iex in aex.InnerExceptions)
                {
                    GetOpsMessagesRecursive(iex, messages);
                }
            }
        }

        private static void GetHttpArgument(object x, out string content, out string type)
        {
            if (x is string sv)
            {
                content = sv;
                type = "string";
                return;
            }

            if (x is bool bv)
            {
                content = bv ? "1" : "0";
                type = "bool";
                return;
            }

            if (x is int iv)
            {
                content = iv.ToString("D", CultureInfo.InvariantCulture);
                type = "int";
                return;
            }

            if (x is long lv)
            {
                content = lv.ToString("D", CultureInfo.InvariantCulture);
                type = "long";
                return;
            }

            if (x is float fv)
            {
                content = fv.ToString("G", CultureInfo.InvariantCulture);
                type = "float";
                return;
            }

            if (x is double dv)
            {
                content = dv.ToString("G", CultureInfo.InvariantCulture);
                type = "double";
                return;
            }

            if (x is decimal dev)
            {
                content = dev.ToString("G", CultureInfo.InvariantCulture);
                type = "decimal";
                return;
            }

            if (x is DateTime dt)
            {
                content = dt.Ticks.ToString("D", CultureInfo.InvariantCulture);
                type = "datetime";
                return;
            }

            if (x is TimeSpan ts)
            {
                content = Convert.ToInt64(ts.TotalMilliseconds).ToString("D", CultureInfo.InvariantCulture);
                type = "timespan";
                return;
            }

            content = x.ToString();
            type = x.GetType().Name;
        }

        private void SendDiagnostis(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess caller, IBaseOperation operation, string text, params object[] args)
        {
            try
            {
                var contentBuilder = new StringBuilder();
                contentBuilder.AppendLine(text.Length.ToString("D", CultureInfo.InvariantCulture));
                contentBuilder.AppendLine(text);

                if (args?.Length > 0)
                {
                    contentBuilder.AppendLine(args.Length.ToString("D", CultureInfo.InvariantCulture));
                    foreach (var argument in args)
                    {
                        if (argument == null)
                        {
                            contentBuilder.AppendLine("null,0,1");
                            continue;
                        }

                        GetHttpArgument(argument, out var argContent, out var type);
                        contentBuilder.Append(type).Append(',').Append(argContent.Length).AppendLine(",0");
                        contentBuilder.AppendLine(argContent);
                    }
                }
                else
                {
                    contentBuilder.AppendLine("0");
                }

                var queryValues = new Dictionary<string, string>
                {
                    { "severity", severity.ToString() }
                };

                if (plugin != null)
                {
                    queryValues.Add("contextName", ModuleConfiguration.ModuleName + "," + plugin.Name);
                }
                else
                {
                    queryValues.Add("contextName", ModuleConfiguration.ModuleName);
                }

                if (caller != null)
                {
                    queryValues.Add("callerUid", caller.UID);
                    queryValues.Add("callerName", caller.Name);
                }

                if (operation != null)
                {
                    queryValues.Add("opType", operation.GetType().Name);
                    queryValues.Add("opNum", operation.Number.ToString("D", CultureInfo.InvariantCulture));
                    queryValues.Add("opName", operation.Name);
                }

                if (forOps)
                {
                    queryValues.Add("forOps", "1");
                }

                var fullUrl = AddQueryValues(new Uri(DiagnosticsUri, "log").ToString(), queryValues);

                using var content = new StringContent(contentBuilder.ToString(), Encoding.UTF8, "application/text");

                if (_disgnosticsClient == null)
                {
                    _disgnosticsClient = new HttpClient
                    {
                        Timeout = TimeSpan.FromMilliseconds(100)
                    };
                }

#pragma warning disable CA2234 // Pass system uri objects instead of strings
                var response = _disgnosticsClient.PostAsync(fullUrl, content).Result;
#pragma warning restore CA2234 // Pass system uri objects instead of strings
                var responseBody = response.Content.ReadAsStringAsync().Result;
                if (responseBody != "ACK")
                {
                    throw new Exception("ohh");
                }
            }
            catch (Exception)
            {
            }
        }

        private static string AddQueryValues(string uri, Dictionary<string, string> values)
        {
            var uriLeft = uri;
            string uriRight = null;

            var idx = uri.IndexOf('#', StringComparison.InvariantCulture);
            if (idx != -1)
            {
                uriRight = uri.Substring(idx);
                uriLeft = uri.Substring(0, idx);
            }

            var addSeparator = uriLeft.IndexOf('?', StringComparison.InvariantCulture) == -1;

            var sb = new StringBuilder();
            sb.Append(uriLeft);
            foreach (var kvp in values)
            {
                sb.Append(addSeparator ? '?' : '&')
                .Append(UrlEncoder.Default.Encode(kvp.Key))
                .Append('=')
                .Append(UrlEncoder.Default.Encode(kvp.Value));

                addSeparator = false;
            }

            if (uriRight != null)
                sb.Append(uriRight);

            return sb.ToString();
        }

        private bool _isDisposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _disgnosticsClient?.Dispose();
                }

                _isDisposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void LogCustom(bool forOps, IEtlPlugin plugin, string fileName, IProcess caller, string text, params object[] args)
        {
            var logsFolder = forOps
                ? SerilogConfigurator.OpsLogFolder
                : SerilogConfigurator.DevLogFolder;

            if (!Directory.Exists(logsFolder))
            {
                try
                {
                    Directory.CreateDirectory(logsFolder);
                }
                catch (Exception)
                {
                }
            }

            var filePath = Path.Combine(logsFolder, fileName);

            var line = new StringBuilder()
                .Append(ModuleConfiguration.ModuleName)
                .Append("\t")
                .Append(plugin != null ? plugin.Name + "\t" : "")
                .Append(caller != null ? caller.Name + "\t" : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }
        }
    }
}