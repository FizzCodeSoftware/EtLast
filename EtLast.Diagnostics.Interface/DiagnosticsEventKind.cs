namespace FizzCode.EtLast.Diagnostics.Interface
{
    public enum DiagnosticsEventKind
    {
        TextDictionaryKeyAdded,
        RowValueChanged,
        RowStoreStarted,
        RowStored,
        RowOwnerChanged,
        RowCreated,
        ProcessInvocationStart,
        ProcessInvocationEnd,
        Log,
        IoCommandStart,
        IoCommandEnd,
        ContextEnded,
    }
}
