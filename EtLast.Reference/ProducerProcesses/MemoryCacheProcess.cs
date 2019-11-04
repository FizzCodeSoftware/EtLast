namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public class MemoryCacheProcess : AbstractBaseProducerProcess
    {
        public bool CloneRows { get; set; }

        private bool _firstEvaluationFinished;
        private List<IRow> _cache;

        public MemoryCacheProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IExecutionBlock caller = null)
        {
            Caller = caller;

            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            var startedOn = Stopwatch.StartNew();
            var resultCount = 0;

            if (_cache != null)
            {
                if (!_firstEvaluationFinished)
                {
                    throw new EtlException(this, "the memory cache is not built yet before the second call on " + nameof(MemoryCacheProcess) + "." + nameof(Evaluate));
                }

                Context.Log(LogSeverity.Information, this, "returning rows from cache");
                if (CloneRows)
                {
                    foreach (var row in _cache)
                    {
                        var newRow = Context.CreateRow(row.ColumnCount);
                        foreach (var kvp in row.Values)
                        {
                            newRow.SetValue(kvp.Key, kvp.Value, this);
                        }

                        resultCount++;
                        yield return newRow;
                    }
                }
                else
                {
                    foreach (var row in _cache)
                    {
                        resultCount++;
                        yield return row;
                    }
                }

                Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows from {InputProcess} in {Elapsed}", resultCount, "cache", startedOn.Elapsed);
                yield break;
            }
            else
            {
                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);

                var inputRows = InputProcess.Evaluate(this);
                _cache = new List<IRow>();
                if (CloneRows)
                {
                    foreach (var row in inputRows)
                    {
                        if (IgnoreRowsWithError && row.HasError())
                            continue;

                        _cache.Add(row);
                        var newRow = Context.CreateRow(row.ColumnCount);
                        foreach (var kvp in row.Values)
                        {
                            newRow.SetValue(kvp.Key, kvp.Value, this);
                        }

                        resultCount++;
                        yield return newRow;
                    }
                }
                else
                {
                    foreach (var row in inputRows)
                    {
                        if (IgnoreRowsWithError && row.HasError())
                            continue;

                        _cache.Add(row);
                        resultCount++;
                        yield return row;
                    }
                }

                _firstEvaluationFinished = true;

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", resultCount, InputProcess.Name, startedOn.Elapsed);
            }
        }
    }
}