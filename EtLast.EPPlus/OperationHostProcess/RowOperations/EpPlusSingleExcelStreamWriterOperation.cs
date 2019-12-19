namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusSingleExcelStreamWriterOperation<TState> : AbstractRowOperation
        where TState : BaseExcelWriterState, new()
    {
        public RowTestDelegate If { get; set; }
        public Stream Stream { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage Source { get; set; }

        private ExcelPackage _excelPackage;
        private TState _state;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (_excelPackage == null) // lazy load here instead of prepare
            {
                _excelPackage = Source ?? new ExcelPackage(Stream);
                Initialize?.Invoke(_excelPackage, _state);
            }

            try
            {
                Action.Invoke(row, _excelPackage, _state);

                Process.Context.OnRowStored?.Invoke(Process, this, row, new List<KeyValuePair<string, string>>()
                {
                    new KeyValuePair<string, string>("Sheet", _state.LastWorksheet?.Name),
                });
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(Process, this, row, "error raised during writing an excel stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel stream, message: {0}, row: {1}", ex.Message, row.ToDebugString()));
                throw exception;
            }
        }

        public override void Prepare()
        {
            if (Stream == null)
                throw new OperationParameterNullException(this, nameof(Stream));

            if (Action == null)
                throw new OperationParameterNullException(this, nameof(Action));

            _state = new TState();
        }

        public override void Shutdown()
        {
            Finalize?.Invoke(_excelPackage, _state);

            base.Shutdown();

            _state = default;

            _excelPackage.Save();
            _excelPackage.Dispose();
            _excelPackage = null;
        }
    }
}