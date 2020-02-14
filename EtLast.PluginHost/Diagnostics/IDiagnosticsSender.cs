namespace FizzCode.EtLast.PluginHost
{
    using System;
    using FizzCode.EtLast.Diagnostics.Interface;

    public interface IDiagnosticsSender : IDisposable
    {
        int GetTextDictionaryKey(string text);
        void SendDiagnostics(DiagnosticsEventKind kind, Action<ExtendedBinaryWriter> writerDelegate);
        void Flush();
    }
}