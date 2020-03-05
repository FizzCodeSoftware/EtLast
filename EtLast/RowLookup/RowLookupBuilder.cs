namespace FizzCode.EtLast
{
    using System;

    public class RowLookupBuilder
    {
        public IEvaluable Process { get; set; }
        public RowKeyGenerator KeyGenerator { get; set; }

        public RowLookup Build(IProcess caller)
        {
            var lookup = new RowLookup();
            Append(lookup, caller);
            return lookup;
        }

        public void Append(ICountableLookup lookup, IProcess caller)
        {
            var allRows = Process.Evaluate(caller).TakeRowsAndReleaseOwnership();
            var rowCount = 0;
            foreach (var row in allRows)
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

                lookup.AddRow(key, row);
            }

            caller?.Context.Log(LogSeverity.Debug, caller, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rowCount, lookup.Count);
        }
    }
}