namespace FizzCode.EtLast;

public sealed class RowLookupBuilder
{
    public required ISequence Process { get; init; }
    public required Func<IReadOnlySlimRow, string> KeyGenerator { get; init; }

    public RowLookup Build(IProcess caller)
    {
        var lookup = new RowLookup();
        AddTo(lookup, caller);
        return lookup;
    }

    public void AddTo(ICountableLookup lookup, IProcess caller)
    {
        var allRows = Process.TakeRowsAndReleaseOwnership(caller);
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
                throw KeyGeneratorException.Wrap(caller, row, ex);
            }

            lookup.AddRow(key, row);
        }

        caller?.Context.Log(LogSeverity.Debug, caller, "fetched {RowCount} rows, lookup size is {LookupSize}",
            rowCount, lookup.Count);
    }
}