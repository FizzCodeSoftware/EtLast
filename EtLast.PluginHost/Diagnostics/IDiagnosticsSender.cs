namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.IO;
    using FizzCode.EtLast.Diagnostics.Interface;

    public interface IDiagnosticsSender : IDisposable
    {
        void SendDiagnostics(DiagnosticsEventKind kind, Action<BinaryWriter> writerDelegate);
        void Flush();
    }
}