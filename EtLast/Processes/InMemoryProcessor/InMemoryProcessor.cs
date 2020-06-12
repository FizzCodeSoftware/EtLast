namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public delegate IEnumerable<IRow> InMemoryProcessorDelegate(InMemoryProcessor proc, IReadOnlyList<IRow> rows);

    /// <summary>
    /// Useful only for small amount of data due to all input rows are collected into a List and processed at once.
    /// </summary>
    public class InMemoryProcessor : AbstractEvaluable
    {
        public IEvaluable InputProcess { get; set; }
        public InMemoryProcessorDelegate Action { get; set; }

        public InMemoryProcessor(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            List<IRow> rows;
            try
            {
                rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().ToList();
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                yield break;
            }

            var resultCount = 0;

            netTimeStopwatch.Stop();
            var enumerator = Action.Invoke(this, rows).GetEnumerator();
            netTimeStopwatch.Start();

            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                IRow row;
                try
                {
                    if (!enumerator.MoveNext())
                        break;

                    row = enumerator.Current;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                    break;
                }

                resultCount++;
                netTimeStopwatch.Stop();
                yield return row;
                netTimeStopwatch.Start();
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "processed {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
                resultCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}