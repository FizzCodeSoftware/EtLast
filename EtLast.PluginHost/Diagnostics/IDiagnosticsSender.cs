namespace FizzCode.EtLast.PluginHost
{
    using System;

    public interface IDiagnosticsSender : IDisposable
    {
        void SendDiagnostics(string category, object content);
        void Flush();
    }
}