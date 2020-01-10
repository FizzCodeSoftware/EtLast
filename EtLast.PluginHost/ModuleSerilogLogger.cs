namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;
    using Serilog.Parsing;

    public class ModuleSerilogLogger : IEtlPluginLogger, IDisposable
    {
        public ILogger Logger { get; set; }
        public ILogger OpsLogger { get; set; }
        public ModuleConfiguration ModuleConfiguration { get; set; }
        public Uri DiagnosticsUri { get; set; }

        private HttpClient _diagnosticsClient;
        private readonly object _customFileLock = new object();

        private int _diagnosticsNumber;
        private readonly Dictionary<string, MessageTemplate> _messageTemplateCache = new Dictionary<string, MessageTemplate>();
        private readonly object _messageTemplateCacheLock = new object();
        private readonly MessageTemplateParser _messageTemplateParser = new MessageTemplateParser();

        public void Log(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess process, IBaseOperation operation, string text, params object[] args)
        {
            var ident = "";
            if (process != null)
            {
                var p = process;
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

            if (process != null)
                values.Add(process.Name);

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
                    + (process != null ? "<{ActiveProcess}> " : "")
                    + (operation != null ? "({Operation}) " : "")
                    + text,
                values.ToArray());

            if (DiagnosticsUri != null)
            {
                SendDiagnostis(severity, forOps, plugin, process, operation, text, args);
            }
        }

        public void LogException(IEtlPlugin plugin, ContextExceptionEventArgs args)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(args.Exception, opsErrors);
            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, plugin, args.Process, args.Operation, opsError);
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

            Log(LogSeverity.Fatal, false, plugin, args.Process, args.Operation, "{Message}", msg);
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

        private void SendDiagnostis(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess process, IBaseOperation operation, string text, params object[] args)
        {
            if (args.Length == 0)
            {
                SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
                {
                    Timestamp = DateTime.Now.Ticks,
                    Text = text,
                    Severity = severity,
                    ContextName = plugin != null
                        ? new string[] { ModuleConfiguration.ModuleName, plugin.Name }
                        : new string[] { ModuleConfiguration.ModuleName },
                    ForOps = forOps,
                    ProcessName = process?.Name,
                    ProcessUid = process?.UID,
                    OperationName = operation?.Name,
                    OperationType = operation != null ? TypeHelpers.GetFriendlyTypeName(operation.GetType()) : null,
                    OperationNumber = operation?.Number,
                });

                return;
            }

            var template = GetMessageTemplate(text);

            var arguments = new Diagnostics.Interface.NamedArgument[args.Length];
            var idx = 0;
            var tokens = template.Tokens.ToList();
            for (var i = 0; i < tokens.Count && idx < args.Length; i++)
            {
                if (tokens[i] is PropertyToken pt)
                {
                    var rawText = text.Substring(pt.StartIndex, pt.Length);
                    // todo: replace rawText with pt.PropertyName in the original text to remove the unnecessary optional alignment and other attributes
                    arguments[idx] = Diagnostics.Interface.NamedArgument.FromObject(rawText, args[idx]);
                    idx++;
                }
            }

            SendDiagnostics("log", new Diagnostics.Interface.LogEvent()
            {
                Timestamp = DateTime.Now.Ticks,
                Text = text,
                Severity = severity,
                ContextName = plugin != null
                    ? new string[] { ModuleConfiguration.ModuleName, plugin.Name }
                    : new string[] { ModuleConfiguration.ModuleName },
                ForOps = forOps,
                ProcessName = process?.Name,
                ProcessUid = process?.UID,
                OperationName = operation?.Name,
                OperationType = operation != null ? TypeHelpers.GetFriendlyTypeName(operation.GetType()) : null,
                OperationNumber = operation?.Number,
                Arguments = arguments,
            });
        }

        private MessageTemplate GetMessageTemplate(string text)
        {
            if (string.IsNullOrEmpty(text))
                return null;

            lock (_messageTemplateCacheLock)
            {
                if (_messageTemplateCache.TryGetValue(text, out var existingTemplate))
                    return existingTemplate;
            }

            var template = _messageTemplateParser.Parse(text);
            lock (_messageTemplateCacheLock)
            {
                if (!_messageTemplateCache.ContainsKey(text))
                {
                    if (_messageTemplateCache.Count == 1000)
                        _messageTemplateCache.Clear();

                    _messageTemplateCache[text] = template;
                }
            }

            return template;
        }

        private void SendDiagnostics(string relativeUri, object content)
        {
            try
            {
                var num = Interlocked.Increment(ref _diagnosticsNumber);

                var fullUri = new Uri(DiagnosticsUri, relativeUri + "?num=" + num);
                var jsonContent = JsonSerializer.Serialize(content);

                if (_diagnosticsClient == null)
                {
                    _diagnosticsClient = new HttpClient
                    {
                        Timeout = TimeSpan.FromMilliseconds(100)
                    };
                }

                using var textContent = new StringContent(jsonContent, Encoding.UTF8, "application/json");

                var response = _diagnosticsClient.PostAsync(fullUri, textContent).Result;
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

        private bool _isDisposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _diagnosticsClient?.Dispose();
                }

                _isDisposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void LogCustom(bool forOps, IEtlPlugin plugin, string fileName, IProcess process, string text, params object[] args)
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
                .Append(process != null ? process.Name + "\t" : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }
        }

        public void LifecycleRowCreated(IEtlPlugin plugin, IRow row, IProcess creatorProcess)
        {
            SendDiagnostics("row-created", new Diagnostics.Interface.RowCreatedEvent()
            {
                ContextName = plugin != null
                   ? new string[] { ModuleConfiguration.ModuleName, plugin.Name }
                   : new string[] { ModuleConfiguration.ModuleName },
                ProcessUid = creatorProcess?.UID,
                ProcessName = creatorProcess?.Name,
                RowUid = row.UID,
                Values = row.Values.Select(x => Diagnostics.Interface.NamedArgument.FromObject(x.Key, x.Value)).ToList(),
            });
        }

        public void LifecycleRowOwnerChanged(IEtlPlugin plugin, IRow row, IProcess previousProcess, IProcess currentProcess)
        {
            SendDiagnostics("row-owner-changed", new Diagnostics.Interface.RowOwnerChangedEvent()
            {
                ContextName = plugin != null
                   ? new string[] { ModuleConfiguration.ModuleName, plugin.Name }
                   : new string[] { ModuleConfiguration.ModuleName },
                RowUid = row.UID,
                PreviousProcessUid = previousProcess?.UID,
                PreviousProcessName = previousProcess?.Name,
                NewProcessUid = currentProcess?.UID,
                NewProcessName = currentProcess?.Name,
            });
        }

        public void LifecycleRowStored(IEtlPlugin plugin, IRow row, List<KeyValuePair<string, string>> location)
        {
        }
    }
}