namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using OfficeOpenXml;

    public enum EpPlusExcelHeaderCellMode { Join, KeepFirst, KeepLast }

    public class EpPlusExcelReaderProcess : AbstractProducerProcess
    {
        public string FileName { get; set; }
        public string SheetName { get; set; }
        public int SheetIndex { get; set; } = -1;

        /// <summary>
        /// Optional, preloaded Excel file. In case this property is provided, the FileName property is used only for logging purposes.
        /// Usage example: reader.PreLoadedFile = new ExcelPackage(new FileInfo(fileName));
        /// </summary>
        public ExcelPackage PreLoadedFile { get; set; }

        public bool TreatEmptyStringAsNull { get; set; }
        public bool IgnoreNullOrEmptyRows { get; set; } = true;

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        public bool Transpose { get; set; }

        public bool Unmerge { get; set; } = true;

        public int[] HeaderRows { get; set; } = new[] { 1 };

        /// <summary>
        /// Default value is <see cref="EpPlusExcelHeaderCellMode.KeepLast"/>
        /// </summary>
        public EpPlusExcelHeaderCellMode HeaderCellMode { get; set; } = EpPlusExcelHeaderCellMode.KeepLast;

        /// <summary>
        /// Default value is "/"
        /// </summary>
        public string HeaderRowJoinSeparator { get; set; } = "/";

        public int FirstDataRow { get; set; } = 2;
        public int FirstDataColumn { get; set; } = 1;

        public EpPlusExcelReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }

        protected override IEnumerable<IRow> Produce(Stopwatch startedOn)
        {
            if (!File.Exists(FileName))
            {
                var exception = new EtlException(this, "input file doesn't exists");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exists: {0}", FileName));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            if (!string.IsNullOrEmpty(SheetName))
                Context.Log(LogSeverity.Debug, this, "reading from {FileName}/{SheetName}", PathHelpers.GetFriendlyPathName(FileName), SheetName);
            else
                Context.Log(LogSeverity.Debug, this, "reading from {FileName}/#{SheetIndex}", PathHelpers.GetFriendlyPathName(FileName), SheetIndex);

            if (Transpose)
            {
                throw new NotImplementedException("Transpose is not finished yet, must be tested before used");
            }

            var columnIndexes = new Dictionary<string, (int Index, ReaderDefaultColumnConfiguration Configuration)>();

            var package = PreLoadedFile;
            if (package == null)
            {
                try
                {
                    Context.Stat.IncrementCounter("excel files opened", 1);
#pragma warning disable CA2000 // Dispose objects before losing scope
#pragma warning disable IDE0068 // Use recommended dispose pattern
                    package = new ExcelPackage(new FileInfo(FileName));
#pragma warning restore IDE0068 // Use recommended dispose pattern
#pragma warning restore CA2000 // Dispose objects before losing scope
                }
                catch (Exception ex)
                {
                    var exception = new EtlException(this, "excel file read failed", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}", FileName, ex.Message));
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }

            try
            {
                package.Compatibility.IsWorksheets1Based = false;
                var workbook = package.Workbook;
                if (workbook == null || workbook.Worksheets.Count == 0)
                    yield break;

                var sheet = !string.IsNullOrEmpty(SheetName) ? workbook.Worksheets[SheetName] : workbook.Worksheets[SheetIndex];
                if (sheet == null)
                {
                    if (!string.IsNullOrEmpty(SheetName))
                    {
                        var exception = new EtlException(this, "can't read excel sheet");
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't read excel sheet, file name: {0}, sheet: {1}, existing sheet names: {2}", FileName, SheetName, string.Join(",", workbook.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", FileName);
                        exception.Data.Add("SheetName", SheetName);
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook.Worksheets.Select(x => x.Name)));
                        throw exception;
                    }

                    yield break;
                }

                var rowCount = sheet.Dimension.End.Row;

                var endColumn = !Transpose ? sheet.Dimension.End.Column : sheet.Dimension.End.Row;
                var endRow = !Transpose ? sheet.Dimension.End.Row : sheet.Dimension.End.Column;

                var excelColumns = new List<string>();

                for (var colIndex = FirstDataColumn; colIndex <= endColumn; colIndex++)
                {
                    var excelColumn = "";

                    if (!Transpose)
                    {
                        for (var headerRowIndex = 0; headerRowIndex < HeaderRows.Length; headerRowIndex++)
                        {
                            var ri = HeaderRows[headerRowIndex];

                            var c = GetCellUnmerged(sheet, ri, colIndex)?.Value?.ToString();
                            if (!string.IsNullOrEmpty(c))
                            {
                                if (HeaderCellMode == EpPlusExcelHeaderCellMode.Join)
                                {
                                    excelColumn += (!string.IsNullOrEmpty(excelColumn) ? HeaderRowJoinSeparator : "") + c;
                                }
                                else if (HeaderCellMode == EpPlusExcelHeaderCellMode.KeepFirst)
                                {
                                    if (string.IsNullOrEmpty(excelColumn))
                                        excelColumn = c;
                                }
                                else
                                {
                                    excelColumn = c;
                                }
                            }
                        }
                    }
                    else
                    {
                        // support transpose here...
                    }

                    if (string.IsNullOrEmpty(excelColumn))
                        continue;

                    excelColumn = EnsureDistinctColumnNames(excelColumns, excelColumn);

                    var columnConfiguration = ColumnConfiguration.Find(x => string.Equals(x.SourceColumn, excelColumn, StringComparison.InvariantCultureIgnoreCase));
                    if (columnConfiguration != null)
                    {
                        var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                        columnIndexes.Add(column, (colIndex, columnConfiguration));
                    }
                    else if (DefaultColumnConfiguration != null)
                    {
                        columnIndexes.Add(excelColumn, (colIndex, DefaultColumnConfiguration));
                    }
                }

                for (var rowIndex = FirstDataRow; rowIndex <= endRow; rowIndex++)
                {
                    Context.Stat.IncrementCounter("excel rows read", 1);

                    var row = Context.CreateRow(columnIndexes.Count);

                    if (IgnoreNullOrEmptyRows)
                    {
                        var empty = true;
                        foreach (var kvp in columnIndexes)
                        {
                            var ri = !Transpose ? rowIndex : kvp.Value.Index;
                            var ci = !Transpose ? kvp.Value.Index : rowIndex;

                            if (GetCellUnmerged(sheet, ri, ci)?.Value != null)
                            {
                                empty = false;
                                break;
                            }
                        }

                        if (empty)
                            continue;
                    }

                    foreach (var kvp in columnIndexes)
                    {
                        var ri = !Transpose ? rowIndex : kvp.Value.Index;
                        var ci = !Transpose ? kvp.Value.Index : rowIndex;

                        var value = GetCellUnmerged(sheet, ri, ci)?.Value;
                        if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                        {
                            value = null;
                        }

                        value = HandleConverter(this, value, kvp.Key, kvp.Value.Configuration, row, out var error);
                        if (error)
                            continue;

                        row.SetValue(kvp.Key, value, this);
                    }

                    if (IgnoreNullOrEmptyRows && row.IsNullOrEmpty())
                        continue;

                    yield return row;
                }
            }
            finally
            {
                if (PreLoadedFile == null)
                {
                    package.Dispose();
                    package = null;
                }
            }
        }

        private static string EnsureDistinctColumnNames(List<string> excelColumns, string excelColumn)
        {
            var col = excelColumn;
            var i = 1;
            while (excelColumns.Contains(col))
            {
                col = excelColumn + i.ToString("D", CultureInfo.InvariantCulture);
                i++;
            }

            excelColumns.Add(col);
            return col;
        }

        private ExcelRange GetCellUnmerged(ExcelWorksheet sheet, int row, int col)
        {
            if (!Unmerge)
                return sheet.Cells[row, col];

            var mergedCellAddress = sheet.MergedCells[row, col];
            if (mergedCellAddress == null)
                return sheet.Cells[row, col];

            var address = new ExcelAddress(mergedCellAddress);
            return sheet.Cells[address.Start.Address];
        }
    }
}