namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusSimpleRowWriterMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string FileName { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        public string SheetName { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; set; }

        public EpPlusSimpleRowWriterMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var state = new SimpleExcelWriterState();
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
                    new KeyValuePair<string, string>("File", PathHelpers.GetFriendlyPathName(FileName)),
                    new KeyValuePair<string, string>("Sheet", SheetName),
                });

                if (package == null) // lazy load here instead of prepare
                {
#pragma warning disable CA2000 // Dispose objects before losing scope
                    package = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
#pragma warning restore CA2000 // Dispose objects before losing scope

                    state.LastWorksheet = package.Workbook.Worksheets.Add(SheetName);
                    state.LastRow = 1;
                    state.LastCol = 1;
                    foreach (var col in ColumnConfiguration)
                    {
                        state.LastWorksheet.Cells[state.LastRow, state.LastCol].Value = col.ToColumn;
                        state.LastCol++;
                    }

                    state.LastRow++;
                }

                try
                {
                    state.LastCol = 1;
                    foreach (var col in ColumnConfiguration)
                    {
                        state.LastWorksheet.Cells[state.LastRow, state.LastCol].Value = row[col.FromColumn];
                        state.LastCol++;
                    }

                    state.LastRow++;
                }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}", FileName, ex.Message, row.ToDebugString()));
                    exception.Data.Add("FileName", FileName);
                    exception.Data.Add("SheetName", SheetName);
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

            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (string.IsNullOrEmpty(SheetName))
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}