namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Serilog;
    using Serilog.Events;

    internal class EtlSessionSerilogAdapter : IEtlContextListener
    {
        private readonly ILogger _logger;
        private readonly ILogger _opsLogger;
        private readonly ILogger _ioLogger;
        private readonly string _devLogFolder;
        private readonly string _opsLogFolder;
        private readonly object _customFileLock = new();

        public EtlSessionSerilogAdapter(ILogger logger, ILogger opsLogger, ILogger ioLogger, string devLogFolder, string opsLogFolder)
        {
            _logger = logger;
            _opsLogger = opsLogger;
            _ioLogger = ioLogger;
            _devLogFolder = devLogFolder;
            _opsLogFolder = opsLogFolder;
        }

        public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
        {
            Log(severity, forOps, transactionId, process, text, args);
        }

        public void Log(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
        {
            var sb = new StringBuilder();
            var values = new List<object>();

            if (process != null)
            {
                IEtlTask task = null;
                var proc = process;
                while (proc != null)
                {
                    if (proc is IEtlTask t)
                    {
                        task = t;
                        break;
                    }

                    proc = proc.InvocationInfo.Caller;
                }

                if (task != null)
                {
                    sb.Append("[{ActiveTask}] ");
                    values.Add(task.Name);
                }

                if (process != task)
                {
                    sb.Append("[{ActiveProcess}] ");
                    values.Add(process.Name);
                }

                var topic = process.GetTopic();
                if (topic != null)
                {
                    sb.Append("[{ActiveTopic}] ");
                    values.Add(topic);
                }
            }

            if (transactionId != null)
            {
                sb.Append("/{ActiveTransaction}/ ");
                values.Add(transactionId);
            }

            sb.Append(text);
            if (args != null)
                values.AddRange(args);

            var logger = forOps
                ? _opsLogger
                : _logger;

            logger.Write((LogEventLevel)severity, sb.ToString(), values.ToArray());
        }

        public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
        {
            var logsFolder = forOps
               ? _opsLogFolder
               : _devLogFolder;

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

            var topic = process?.GetTopic();

            var line = new StringBuilder()
                .Append(!string.IsNullOrEmpty(topic)
                    ? topic + "\t"
                    : "")
                .Append(process != null
                    ? process.Name + "\t"
                    : "")
                .AppendFormat(CultureInfo.InvariantCulture, text, args)
                .ToString();

            lock (_customFileLock)
            {
                File.AppendAllText(filePath, line + Environment.NewLine);
            }
        }

        public void OnException(IProcess process, Exception exception)
        {
            var opsErrors = new List<string>();
            GetOpsMessagesRecursive(exception, opsErrors);
            if (opsErrors.Count == 0)
                opsErrors.Add("error happened, please check the full log file for details");

            foreach (var opsError in opsErrors)
            {
                Log(LogSeverity.Fatal, true, null, process, opsError);
            }

            var msg = exception.FormatExceptionWithDetails();
            Log(LogSeverity.Fatal, false, null, process, "{ErrorMessage}", msg);
        }

        public void GetOpsMessagesRecursive(Exception ex, List<string> messages)
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

        public void OnContextIoCommandStart(int uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
        {
            if (message != null)
            {
                var sb = new StringBuilder();
                var values = new List<object>();

                if (process != null)
                {
                    var topic = process.GetTopic();
                    if (topic != null)
                    {
                        sb.Append("[{ActiveProcess}/{ActiveTopic}] ");
                        values.Add(process.Name);
                        values.Add(topic);
                    }
                    else
                    {
                        sb.Append("[{ActiveProcess}] ");
                        values.Add(process.Name);
                    }
                }

                if (transactionId != null)
                {
                    sb.Append("/{ActiveTransaction}/ ");
                    values.Add(transactionId);
                }

                sb.Append("{IoCommandUid}/{IoCommandKind}");
                values.Add("#" + uid.ToString("D", CultureInfo.InvariantCulture));
                values.Add(kind.ToString());

                if (location != null)
                {
                    sb.Append(", location: {IoCommandTarget}");
                    values.Add(location);
                }

                if (path != null)
                {
                    sb.Append(", path: {IoCommandTargetPath}");
                    values.Add(path);
                }

                if (timeoutSeconds != null)
                {
                    sb.Append(", timeout: {IoCommandTimeout}");
                    values.Add(timeoutSeconds);
                }

                sb.Append(", message: ").Append(message);
                if (messageArgs != null)
                    values.AddRange(messageArgs);

                if (command != null)
                {
                    sb.Append(", command: {IoCommand}");
                    values.Add(command);
                }

                _ioLogger.Write(LogEventLevel.Verbose, sb.ToString(), values.ToArray());
            }
        }

        public void OnContextIoCommandEnd(IProcess process, int uid, IoCommandKind kind, int? affectedDataCount, Exception ex)
        {
            if (ex != null)
            {
                var sb = new StringBuilder();
                var values = new List<object>();

                if (process != null)
                {
                    var topic = process.GetTopic();
                    if (topic != null)
                    {
                        // todo: we need task capture somehow...
                        sb.Append("[{ActiveProcess}/{ActiveTopic}] ");
                        values.Add(process.Name);
                        values.Add(topic);
                    }
                    else
                    {
                        sb.Append("[{ActiveProcess}] ");
                        values.Add(process.Name);
                    }
                }
                else
                {
                }

                sb.Append("{IoCommandUid}/EXCEPTION, {ErrorMessage}");
                values.Add("#" + uid.ToString("D", CultureInfo.InvariantCulture));
                values.Add(ex.FormatExceptionWithDetails());

                _ioLogger.Write(LogEventLevel.Error, sb.ToString(), values.ToArray());
            }
        }

        public void OnRowCreated(IReadOnlyRow row)
        {
        }

        public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
        {
        }

        public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
        {
        }

        public void OnSinkStarted(int sinkUid, string location, string path)
        {
        }

        public void OnWriteToSink(IReadOnlyRow row, int sinkUid)
        {
        }

        public void OnProcessInvocationStart(IProcess process)
        {
        }

        public void OnProcessInvocationEnd(IProcess process)
        {
        }

        public void OnContextClosed()
        {
        }
    }
}