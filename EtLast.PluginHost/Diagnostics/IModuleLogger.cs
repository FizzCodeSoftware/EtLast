namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;

    public interface IModuleLogger
    {
        IDiagnosticsSender DiagnosticsSender { get; set; }
        void Log(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess process, IBaseOperation operation, string text, params object[] args);
        void LogCustom(bool forOps, IEtlPlugin plugin, string fileName, IProcess process, string text, params object[] args);
        void LogException(IEtlPlugin plugin, ContextExceptionEventArgs args);
        void LifecycleRowCreated(IEtlPlugin plugin, IRow row, IProcess creatorProcess);
        void LifecycleRowOwnerChanged(IEtlPlugin plugin, IRow row, IProcess previousProcess, IProcess currentProcess);
        void LifecycleRowStored(IEtlPlugin plugin, IRow row, List<KeyValuePair<string, string>> location);
        void LifecycleRowValueChanged(IEtlPlugin plugin, IRow row, string column, object previousValue, object newValue, IProcess process, IBaseOperation operation);
    }
}