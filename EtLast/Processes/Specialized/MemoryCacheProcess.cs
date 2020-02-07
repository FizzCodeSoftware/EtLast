namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class MemoryCacheProcess : AbstractProducerProcess
    {
        private bool _firstEvaluationFinished;
        private List<IRow> _cache;

        public MemoryCacheProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
            AutomaticallyEvaluateAndYieldInputProcessRows = false;
        }

        public override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }

        protected override IEnumerable<IRow> Produce()
        {
            if (_cache != null)
            {
                if (!_firstEvaluationFinished)
                {
                    throw new EtlException(this, "the memory cache is not built yet before the second call on " + nameof(MemoryCacheProcess) + "." + nameof(Evaluate));
                }

                Context.Log(LogSeverity.Information, this, "returning rows from cache");
                foreach (var row in _cache)
                {
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    var newRow = Context.CreateRow(this, row.Values);

                    CounterCollection.IncrementCounter("row memory cache hit - clone", 1);
                    yield return newRow;
                }

                yield break;
            }
            else
            {
                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);

                _cache = new List<IRow>();
                var inputRows = InputProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
                foreach (var row in inputRows)
                {
                    if (IgnoreRowsWithError && row.HasError())
                        continue;

                    _cache.Add(row);

                    var newRow = Context.CreateRow(this, row.Values);
                    yield return newRow;
                }

                _firstEvaluationFinished = true;
            }
        }
    }
}