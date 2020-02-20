namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public abstract class AbstractMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        protected AbstractMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl()
        {
            StartMutator();

            var mutatedRows = new List<IRow>();

            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership();
            foreach (var row in rows)
            {
                var apply = false;
                try
                {
                    apply = If?.Invoke(row) != false;
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, ex);
                    Context.AddException(this, exception);
                    break;
                }

                if (!apply)
                {
                    CounterCollection.IncrementCounter("skipped", 1, true);
                    yield return row;
                    continue;
                }

                CounterCollection.IncrementCounter("processed", 1, true);

                var kept = false;
                try
                {
                    foreach (var mutatedRow in MutateRow(row))
                    {
                        if (mutatedRow == row)
                            kept = true;

                        if (mutatedRow.HasStaging)
                            throw new ProcessExecutionException(this, mutatedRow, "unfinished staging");

                        if (mutatedRow.CurrentProcess != this)
                            throw new ProcessExecutionException(this, mutatedRow, "mutator returned a row without ownership");

                        mutatedRows.Add(mutatedRow);
                    }
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, ex);
                    Context.AddException(this, exception);
                    continue;
                }

                if (!kept)
                {
                    Context.SetRowOwner(row, null);
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

        public IEnumerator<IMutator> GetEnumerator()
        {
            yield return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            yield return this;
        }
    }
}