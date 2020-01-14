namespace FizzCode.EtLast.PluginHost
{
    public interface IModuleLogger
    {
        IDiagnosticsSender DiagnosticsSender { get; set; }

        IEtlPlugin CurrentPlugin { get; }
        void SetCurrentPlugin(IEtlPlugin plugin);
        void SetupContextEvents(IEtlContext context);

        void Log(LogSeverity severity, bool forOps, IProcess process, IBaseOperation operation, string text, params object[] args);
    }
}