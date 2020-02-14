namespace FizzCode.EtLast.Diagnostics.Interface
{
    public enum DiagnosticsEventKind
    {
        TextDictionaryKeyAdded,
        RowValueChanged,
        RowStored,
        RowOwnerChanged,
        RowCreated,
        ProcessInvocationStart,
        ProcessInvocationEnd,
        Log,
        DataStoreCommand,
        ContextCountersUpdated,
    }
}
