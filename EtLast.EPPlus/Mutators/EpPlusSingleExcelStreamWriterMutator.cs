namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusSingleExcelStreamWriterMutator<TState> : AbstractEvaluableProcess, IMutator
        where TState : BaseExcelWriterState, new()
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public Stream Stream { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage ExistingPackage { get; set; }

        public EpPlusSingleExcelStreamWriterMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var state = new TState();
            ExcelPackage package = null;

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

                Context.OnRowStored?.Invoke(this, row, new List<KeyValuePair<string, string>>()
                {
                    new KeyValuePair<string, string>("Sheet", state.LastWorksheet?.Name),
                });

                if (package == null) // lazy load here instead of prepare
                {
#pragma warning disable CA2000 // Dispose objects before losing scope
                    package = ExistingPackage ?? new ExcelPackage(Stream);
#pragma warning restore CA2000 // Dispose objects before losing scope
                    Initialize?.Invoke(package, state);
                }

                try
                {
                    Action.Invoke(row, package, state);
                }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, "error raised during writing an excel stream", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel stream, message: {0}, row: {1}",
                        ex.Message, row.ToDebugString()));

                    exception.Data.Add("FileName", Stream);
                    throw exception;
                }

                yield return row;
            }

            if (state.LastWorksheet != null)
            {
                Finalize?.Invoke(package, state);
            }

            if (ExistingPackage == null && package != null)
            {
                package.Save();
                package.Dispose();
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (Stream == null)
                throw new ProcessParameterNullException(this, nameof(Stream));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }
    }
}