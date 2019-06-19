namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using OfficeOpenXml;

    public class EpPlusExcelReaderProcess : AbstractBaseProducerProcess, IEpPlusExcelReaderProcess
    {
        public string FileName { get; set; }
        public string SheetName { get; set; }
        public int SheetIndex { get; set; } = -1;

        /// <summary>
        /// Optional, preloaded Excel file. In case this property is provided, the FileName property is used only for logging purposes.
        /// Usage example: reader.PreLoadedFile = new ExcelPackage(new FileInfo(fileName));
        /// </summary>
        public ExcelPackage PreLoadedFile { get; set; }

        /// <summary>
        /// First row index is (integer) 1
        /// </summary>
        public string AddRowIndexToColumn { get; set; }

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
        { }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));
            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            var relativeFileName = FileName;
            if (!FileName.StartsWith(".") && !FileName.StartsWith(Path.DirectorySeparatorChar.ToString()))
            {
                try
                {
                    var baseFolder = Path.GetDirectoryName((Assembly.GetEntryAssembly() ?? Assembly.GetCallingAssembly()).Location);
                    if (!baseFolder.EndsWith(Path.DirectorySeparatorChar.ToString()))
                        baseFolder += Path.DirectorySeparatorChar;
                    relativeFileName = new Uri(baseFolder).MakeRelativeUri(new Uri(FileName)).OriginalString.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
                }
                catch (Exception) { }
            }

            var sw = Stopwatch.StartNew();

            var evaluateInputProcess = EvaluateInputProcess(sw, (row, rowCount, process) =>
            {
                if (AddRowIndexToColumn != null)
                {
                    row.SetValue(AddRowIndexToColumn, rowCount, process);
                }
            });

            var index = 0;
            foreach (var row in evaluateInputProcess)
            {
                index++;
                yield return row;
            }

            if (!File.Exists(FileName))
            {
                var exception = new EtlException(this, "excel file doesn't exists");
                exception.AddOpsMessage(string.Format("excel file doesn't exists, file name: {0}", FileName));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            var resultCount = 0;
            if (!string.IsNullOrEmpty(SheetName))
                Context.Log(LogSeverity.Information, this, "reading from {RelativeFileName}/{SheetName}", relativeFileName, SheetName);
            else
                Context.Log(LogSeverity.Information, this, "reading from {RelativeFileName}/#{SheetIndex}", relativeFileName, SheetIndex);

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
                    package = new ExcelPackage(new FileInfo(FileName));
                }
                catch (Exception ex)
                {
                    var exception = new EtlException(this, "excel file read failed", ex);
                    exception.AddOpsMessage(string.Format("excel file read failed, file name: {0}, message {1}", FileName, ex.Message));
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
                        exception.AddOpsMessage(string.Format("can't read excel sheet, file name: {0}, sheet: {1}, existing sheet names: {2}", FileName, SheetName, string.Join(",", workbook.Worksheets.Select(x => x.Name))));
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
                    var excelColumn = string.Empty;

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
                                    excelColumn += (!string.IsNullOrEmpty(excelColumn) ? HeaderRowJoinSeparator : string.Empty) + c;
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

                    var columnConfiguration = ColumnConfiguration.Find(x => string.Compare(x.SourceColumn, excelColumn, true) == 0);
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
                        if (value != null && TreatEmptyStringAsNull && (value is string str) && str == string.Empty)
                        {
                            value = null;
                        }

                        value = ReaderProcessHelper.HandleConverter(this, value, ri, kvp.Key, kvp.Value.Configuration, row, out var error);
                        if (error)
                            continue;

                        row.SetValue(kvp.Key, value, this);
                    }

                    if (IgnoreNullOrEmptyRows && row.IsNullOrEmpty())
                        continue;

                    resultCount++;
                    index++;
                    if (AddRowIndexToColumn != null)
                        row.SetValue(AddRowIndexToColumn, index, this);
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

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
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