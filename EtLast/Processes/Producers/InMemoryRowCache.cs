namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public sealed class InMemoryRowCache : AbstractRowSource
    {
        private bool _firstEvaluationFinished;
        private List<IReadOnlySlimRow> _cache;

        /// <summary>
        /// The process evaluates and yields the rows from the input process.
        /// </summary>
        public IProducer InputProcess { get; set; }

        public InMemoryRowCache(IEtlContext context, string topic, string name)
            : base(context, topic, name)
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

                    var newRow = Context.CreateRow(this, row);
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

                    var newRow = Context.CreateRow(this, row);
                    yield return newRow;
                }

                _firstEvaluationFinished = true;
            }
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class InMemoryRowCacheFluent
    {
        public static IFluentProcessMutatorBuilder ReadFromInMemoryRowCache(this IFluentProcessBuilder builder, InMemoryRowCache cache)
        {
            return builder.ReadFrom(cache);
        }

        public static IProducer BuildToInMemoryRowCache(this IFluentProcessMutatorBuilder builder, IEtlContext context, string topic, string name)
        {
            return new InMemoryRowCache(context, topic, name)
            {
                InputProcess = builder.Build(),
            };
        }
    }
}