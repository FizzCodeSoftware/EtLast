namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusSimpleRowWriterOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string FileName { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        public string SheetName { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; set; }

        private ExcelPackage _excelPackage;
        private SimpleExcelWriterState _state;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            Process.Context.OnRowStored?.Invoke(Process, this, row, new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("File", PathHelpers.GetFriendlyPathName(FileName)),
                new KeyValuePair<string, string>("Sheet", SheetName),
            });

            if (_excelPackage == null) // lazy load here instead of prepare
            {
                _excelPackage = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));

                _state.LastWorksheet = _excelPackage.Workbook.Worksheets.Add(SheetName);
                _state.LastRow = 1;
                _state.LastCol = 1;
                foreach (var col in ColumnConfiguration)
                {
                    _state.LastWorksheet.Cells[_state.LastRow, _state.LastCol].Value = col.ToColumn;
                    _state.LastCol++;
                }

                _state.LastRow++;
            }

            try
            {
                _state.LastCol = 1;
                foreach (var col in ColumnConfiguration)
                {
                    _state.LastWorksheet.Cells[_state.LastRow, _state.LastCol].Value = row[col.FromColumn];
                    _state.LastCol++;
                }

                _state.LastRow++;
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(Process, this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}", FileName, ex.Message, row.ToDebugString()));
                exception.Data.Add("FileName", FileName);
                exception.Data.Add("SheetName", SheetName);
                throw exception;
            }
        }

        protected override void PrepareImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new OperationParameterNullException(this, nameof(FileName));

            if (string.IsNullOrEmpty(SheetName))
                throw new OperationParameterNullException(this, nameof(SheetName));

            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));

            _state = new SimpleExcelWriterState();
        }

        public override void Shutdown()
        {
            if (_state.LastWorksheet != null)
            {
                Finalize?.Invoke(_excelPackage, _state);
            }

            base.Shutdown();

            _state = null;

            if (ExistingPackage == null && _excelPackage != null)
            {
                _excelPackage.Save();
                _excelPackage.Dispose();
                _excelPackage = null;
            }
        }
    }
}