namespace FizzCode.EtLast;

public delegate ISequence ProcessCreatorForRowLookupBuilderDelegate(IReadOnlySlimRow[] filterRows);

public sealed class FilteredRowLookupBuilder
{
    public required ProcessCreatorForRowLookupBuilderDelegate ProcessCreator { get; init; }
    public required Func<IReadOnlySlimRow, string> KeyGenerator { get; init; }

    public RowLookup Build(IProcess caller, IReadOnlySlimRow[] filterRows)
    {
        var lookup = new RowLookup();
        AddTo(lookup, caller, filterRows);
        return lookup;
    }

    public void AddTo(ICountableLookup lookup, IProcess caller, IReadOnlySlimRow[] filterRows)
    {
        var process = ProcessCreator.Invoke(filterRows);

        var rows = process.TakeRowsAndReleaseOwnership(caller);
        var rowCount = 0;
        foreach (var row in rows)
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

            if (string.IsNullOrEmpty(key))
                continue;

            lookup.AddRow(key, row);
        }

        caller?.Context.Log(LogSeverity.Debug, caller, "fetched {RowCount} rows, lookup size is {LookupSize}",
            rowCount, lookup.Count);
    }
}