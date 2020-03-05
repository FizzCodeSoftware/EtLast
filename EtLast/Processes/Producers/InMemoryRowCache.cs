namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class InMemoryRowCache : AbstractProducer
    {
        private bool _firstEvaluationFinished;
        private List<IReadOnlySlimRow> _cache;

        /// <summary>
        /// The process evaluates and yields the rows from the input process.
        /// </summary>
        public IEvaluable InputProcess { get; set; }

        public InMemoryRowCache(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
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
                    throw new ProcessExecutionException(this, "the memory cache is not built yet before the second call on " + nameof(InMemoryRowCache) + "." + nameof(Evaluate));
                }

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
                _cache = new List<IReadOnlySlimRow>();
                var inputRows = InputProcess.Evaluate(this).TakeRowsAndReleaseOwnership();
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