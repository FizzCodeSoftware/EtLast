namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusSingleExcelFileWriterOperation<TState> : AbstractRowOperation
        where TState : BaseExcelWriterState, new()
    {
        public RowTestDelegate If { get; set; }
        public string FileName { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage ExistingPackage { get; set; }

        private ExcelPackage _excelPackage;
        private TState _state;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (_excelPackage == null) // lazy load here instead of prepare
            {
                _excelPackage = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
                Initialize?.Invoke(_excelPackage, _state);
            }

            try
            {
                Action.Invoke(row, _excelPackage, _state);

                Process.Context.OnRowStored?.Invoke(row, new List<KeyValuePair<string, string>>()
                {
                    new KeyValuePair<string, string>("File", PathHelpers.GetFriendlyPathName(FileName)),
                    new KeyValuePair<string, string>("Sheet", _state.LastWorksheet?.Name),
                });
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(Process, this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}", FileName, ex.Message, row.ToDebugString()));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new OperationParameterNullException(this, nameof(FileName));

            if (Action == null)
                throw new OperationParameterNullException(this, nameof(Action));

            _state = new TState();
        }

        public override void Shutdown()
        {
            Finalize?.Invoke(_excelPackage, _state);

            base.Shutdown();

            _state = default;

            if (ExistingPackage == null && _excelPackage != null)
            {
                _excelPackage.Save();
                _excelPackage.Dispose();
                _excelPackage = null;
            }
        }
    }
}