namespace FizzCode.EtLast.PluginHost
{
    internal interface ISessionLogger
    {
        IDiagnosticsSender DiagnosticsSender { get; set; }

        IEtlPlugin CurrentPlugin { get; }
        void SetCurrentPlugin(Module module, IEtlPlugin plugin);
        void SetupContextEvents(IEtlContext context);

        void Log(LogSeverity severity, bool forOps, IProcess process, IBaseOperation operation, string text, params object[] args);
    }
}