namespace FizzCode.EtLast
{
    public interface IEtlPluginLogger
    {
        void Log(LogSeverity severity, bool forOps, IEtlPlugin plugin, IProcess caller, IBaseOperation operation, string text, params object[] args);
        void LogException(IEtlPlugin plugin, ContextExceptionEventArgs args);
    }
}