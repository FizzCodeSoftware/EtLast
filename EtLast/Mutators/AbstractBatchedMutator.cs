namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public abstract class AbstractBatchedMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        public abstract int BatchSize { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        protected bool UseBatchKeys { get; set; }

        protected AbstractBatchedMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl()
        {
            StartMutator();

            var mutatedRows = new List<IRow>();
            var removedRows = new List<IRow>();

            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);

            var batch = new List<IRow>();
            var batchKeys = new HashSet<string>();

            var failed = false;

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
                    failed = true;
                    break;
                }

                if (!apply)
                {
                    CounterCollection.IncrementCounter("skipped", 1, true);
                    yield return row;
                    continue;
                }

                CounterCollection.IncrementCounter("processed", 1, true);

                var mutationHappened = false;
                var removeOriginal = false;
                try
                {
                    MutateRow(row, mutatedRows, out removeOriginal, out mutationHappened);
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, ex);
                    Context.AddException(this, exception);
                    continue;
                }

                if (mutationHappened)
                {
                    if (removeOriginal)
                    {
                        Context.SetRowOwner(row, null);
                    }

                    foreach (var mutatedRow in mutatedRows)
                    {
                        if (mutatedRow.CurrentProcess != this)
                            throw new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership");

                        yield return mutatedRow;
                    }

                    mutatedRows.Clear();
                }
                else
                {
                    batch.Add(row);

                    if (UseBatchKeys)
                    {
                        var key = GetBatchKey(row);
                        batchKeys.Add(key);
                    }

                    if ((UseBatchKeys && batchKeys.Count >= BatchSize) || (!UseBatchKeys && batch.Count >= BatchSize))
                    {
                        try
                        {
                            MutateBatch(batch, mutatedRows, removedRows);
                        }
                        catch (ProcessExecutionException) { throw; }
                        catch (Exception ex)
                        {
                            var exception = new ProcessExecutionException(this, ex);
                            Context.AddException(this, exception);
                            failed = true;
                            break;
                        }

                        foreach (var removedRow in removedRows)
                        {
                            Context.SetRowOwner(removedRow, null);
                        }

                        foreach (var mutatedRow in mutatedRows)
                        {
                            if (mutatedRow.CurrentProcess != this)
                                throw new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership");

                            yield return mutatedRow;
                        }

                        mutatedRows.Clear();
                        removedRows.Clear();
                        batch.Clear();
                        batchKeys.Clear();
                    }
                }
            }

            if (batch.Count > 0 && !failed)
            {
                try
                {
                    MutateBatch(batch, mutatedRows, removedRows);
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, ex);
                    Context.AddException(this, exception);
                    failed = true;
                }

                if (!failed)
                {
                    foreach (var removedRow in removedRows)
                    {
                        Context.SetRowOwner(removedRow, null);
                    }

                    foreach (var mutatedRow in mutatedRows)
                    {
                        yield return mutatedRow;
                    }
                }

                mutatedRows.Clear();
                removedRows.Clear();
                batch.Clear();
                batchKeys.Clear();
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

        protected abstract void MutateRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed);

        protected abstract void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows);

        protected virtual string GetBatchKey(IRow row)
        {
            return null;
        }

        public IEnumerable<IMutator> GetMutators()
        {
            yield return this;
        }

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