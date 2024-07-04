namespace FizzCode.EtLast.Diagnostics.Interface;

public enum DiagnosticsEventKind
{
    RowValueChanged,
    SinkStarted,
    WriteToSink,
    RowOwnerChanged,
    RowCreated,
    ProcessStart,
    ProcessEnd,
    Log,
    IoCommandStart,
    IoCommandEnd,
    ContextEnded,
}
