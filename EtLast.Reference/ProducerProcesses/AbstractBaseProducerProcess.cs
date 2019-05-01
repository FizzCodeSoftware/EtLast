namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractBaseProducerProcess : IProcess
    {
        public IEtlContext Context { get; }
        public string Name { get; }
        public IProcess Caller { get; protected set; }
        public IProcess InputProcess { get; set; }

        protected AbstractBaseProducerProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public abstract IEnumerable<IRow> Evaluate(IProcess caller = null);

        protected IEnumerable<IRow> EvaluateInputProcess(Stopwatch sw, Action<IRow, object, IProcess> inputRowAction = null)
        {
            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

                var inputRows = InputProcess.Evaluate(this);
                var rowCount = 0;
                foreach (var row in inputRows)
                {
                    rowCount++;
                    inputRowAction?.Invoke(row, rowCount, this);
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", rowCount, InputProcess.Name, sw.Elapsed);
            }
        }
    }
}