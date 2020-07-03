namespace FizzCode.EtLast
{
    using System;

    public delegate IEvaluable ProcessCreatorForRowLookupBuilderDelegate(IReadOnlySlimRow[] filterRows);

    public class FilteredRowLookupBuilder
    {
        public ProcessCreatorForRowLookupBuilderDelegate ProcessCreator { get; set; }
        public Func<IReadOnlySlimRow, string> KeyGenerator { get; set; }

        public RowLookup Build(IProcess caller, IReadOnlySlimRow[] filterRows)
        {
            var lookup = new RowLookup();
            Append(lookup, caller, filterRows);
            return lookup;
        }

        public void Append(ICountableLookup lookup, IProcess caller, IReadOnlySlimRow[] filterRows)
        {
            var process = ProcessCreator.Invoke(filterRows);

            var rows = process.Evaluate(caller).TakeRowsAndReleaseOwnership();
            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;

                string key = null;
                try
                {
                    key = KeyGenerator(row);
                }
                catch (EtlException) { throw; }
                catch (Exception)
                {
                    var exception = new ProcessExecutionException(caller, row, nameof(RowLookupBuilder) + " failed");
                    throw exception;
                }

                if (string.IsNullOrEmpty(key))
                    continue;

                lookup.AddRow(key, row);
            }

            caller.Context.Log(LogSeverity.Debug, caller, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rowCount, lookup.Count);
        }
    }
}