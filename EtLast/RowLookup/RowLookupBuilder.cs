namespace FizzCode.EtLast;

public sealed class RowLookupBuilder
{
    public ISequence Process { get; set; }
    public Func<IReadOnlySlimRow, string> KeyGenerator { get; set; }

    public RowLookup Build(IProcess caller)
    {
        var lookup = new RowLookup();
        Append(lookup, caller);
        return lookup;
    }

    public void Append(ICountableLookup lookup, IProcess caller)
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