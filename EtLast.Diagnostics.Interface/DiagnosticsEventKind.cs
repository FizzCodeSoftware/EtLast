namespace FizzCode.EtLast.Diagnostics.Interface
{
    public enum DiagnosticsEventKind
    {
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
