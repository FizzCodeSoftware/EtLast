namespace FizzCode.EtLast.Diagnostics.Interface;

public enum DiagnosticsEventKind
{
    RowValueChanged,
    SinkStarted,
    WriteToSink,
    RowOwnerChanged,
    RowCreated,
    ProcessInvocationStart,
    ProcessInvocationEnd,
    Log,
    IoCommandStart,
    IoCommandEnd,
    ContextEnded,
}
