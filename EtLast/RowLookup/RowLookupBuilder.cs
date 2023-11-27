namespace FizzCode.EtLast;

public sealed class RowLookupBuilder
{
    public required ISequence Process { get; init; }
    public required Func<IReadOnlySlimRow, string> KeyGenerator { get; init; }

    public RowLookup Build(ICaller caller, FlowState flowState = null)
    {
        var lookup = new RowLookup();
        AddTo(lookup, caller, flowState);
        return lookup;
    }

    public void AddTo(ICountableLookup lookup, ICaller caller, FlowState flowState = null)
    {
        var allRows = Process.TakeRowsAndReleaseOwnership(caller, flowState);
        var rowCount = 0;
        foreach (var row in allRows)
        {
            rowCount++;

            string key = null;
            try
            {
                key = KeyGenerator(row);
            }
            catch (Exception ex)
            {
                throw KeyGeneratorException.Wrap(caller as IProcess, row, ex);
            }

            lookup.AddRow(key, row);
        }

        caller?.Context.Log(LogSeverity.Debug, caller as IProcess, "fetched {RowCount} rows, lookup size is {LookupSize}",
            rowCount, lookup.Count);
    }
}