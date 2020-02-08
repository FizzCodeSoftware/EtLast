namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public abstract class AbstractMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        protected bool InvertSkipBehavior { get; set; }

        protected AbstractMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl()
        {
            StartMutator();

            var mutatedRows = new List<IRow>();

            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                var skip = false;
                try
                {
                    skip = If?.Invoke(row) == false;
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, ex);
                    Context.AddException(this, exception);
                }

                if (skip)
                {
                    CounterCollection.IncrementCounter("skipped", 1);
                    yield return row;
                    continue;
                }

                CounterCollection.IncrementCounter("processed", 1);

                try
                {
                    var found = false;
                    foreach (var mutatedRow in MutateRow(row))
                    {
                        if (mutatedRow == row)
                            found = true;

                        mutatedRows.Add(mutatedRow);
                    }

                    if (!found)
                    {
                        Context.SetRowOwner(row, null);
                    }
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, ex);
                    Context.AddException(this, exception);
                    continue;
                }

                foreach (var mutatedRow in mutatedRows)
                {
                    yield return mutatedRow;
                }

                mutatedRows.Clear();
            }

            CloseMutator();
        }


        protected sealed override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            ValidateMutator();
        }

        protected virtual void ValidateMutator()
        {
        }

        protected virtual void StartMutator()
        {
        }

        protected virtual void CloseMutator()
        {
        }

        protected abstract IEnumerable<IRow> MutateRow(IRow row);
    }
}