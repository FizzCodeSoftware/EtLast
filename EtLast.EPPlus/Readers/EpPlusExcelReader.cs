namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using OfficeOpenXml;

    public enum EpPlusExcelHeaderCellMode { Join, KeepFirst, KeepLast }

    public class EpPlusExcelReader : AbstractProducer
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
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; set; } = true;

        /// <summary>
        /// Default true.
        /// </summary>
        public bool AutomaticallyTrimAllStringValues { get; set; } = true;

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        public bool Transpose { get; set; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool Unmerge { get; set; } = true;

        public int[] HeaderRows { get; set; } = new[] { 1 };

        /// <summary>
        /// Default value is <see cref="EpPlusExcelHeaderCellMode.KeepLast"/>
        /// </summary>
        public EpPlusExcelHeaderCellMode HeaderCellMode { get; set; } = EpPlusExcelHeaderCellMode.KeepLast;

        /// <summary>
        /// Default value is "/".
        /// </summary>
        public string HeaderRowJoinSeparator { get; set; } = "/";

        public int FirstDataRow { get; set; } = 2;
        public int FirstDataColumn { get; set; } = 1;

        public EpPlusExcelReader(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }

        protected override IEnumerable<IRow> Produce()
        {
            if (Transpose)
            {
                throw new NotImplementedException("Transpose is not finished yet, must be tested before used");
            }

            var iocUid = !string.IsNullOrEmpty(SheetName)
                ? Context.RegisterIoCommandStart(this, IoCommandKind.fileRead, PathHelpers.GetFriendlyPathName(FileName), SheetName, null, null, null, null,
                    "reading from: {FileName}[{SheetName}]",
                    PathHelpers.GetFriendlyPathName(FileName), SheetName)
                : Context.RegisterIoCommandStart(this, IoCommandKind.fileRead, PathHelpers.GetFriendlyPathName(FileName), "#" + SheetIndex.ToString("D", CultureInfo.InvariantCulture), null, null, null, null,
                    "reading from: {FileName}[{SheetIndex}]",
                    PathHelpers.GetFriendlyPathName(FileName), SheetIndex);

            if (!File.Exists(FileName))
            {
                var exception = new ProcessExecutionException(this, "file doesn't exist");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "file doesn't exist: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                throw exception;
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
                    Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, null, ex);

                    var exception = new ProcessExecutionException(this, "excel file read failed", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}",
                        FileName, ex.Message));
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }

            var rowCount = 0;
            try
            {
                package.Compatibility.IsWorksheets1Based = false;
                var workbook = package.Workbook;

                var sheet = !string.IsNullOrEmpty(SheetName)
                    ? workbook?.Worksheets[SheetName]
                    : workbook?.Worksheets[SheetIndex];

                if (sheet == null)
                {
                    if (!string.IsNullOrEmpty(SheetName))
                    {
                        var exception = new ProcessExecutionException(this, "can't find excel sheet by name");
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't find excel sheet, file name: {0}, sheet name: {1}, existing sheet names: {2}",
                            FileName, SheetName, string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", FileName);
                        exception.Data.Add("SheetName", SheetName);
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                        Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                        throw exception;
                    }
                    else
                    {
                        var exception = new ProcessExecutionException(this, "can't find excel sheet by index");
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't find excel sheet, file name: {0}, sheet index: {1}, existing sheet names: {2}",
                            FileName, SheetIndex.ToString("D", CultureInfo.InvariantCulture), string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", FileName);
                        exception.Data.Add("SheetIndex", SheetIndex.ToString("D", CultureInfo.InvariantCulture));
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                        Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                        throw exception;
                    }
                }

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

                    if (AutomaticallyTrimAllStringValues)
                    {
                        excelColumn = excelColumn.Trim();
                        if (string.IsNullOrEmpty(excelColumn))
                            continue;
                    }

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

                var initialValues = new List<KeyValuePair<string, object>>();

                for (var rowIndex = FirstDataRow; rowIndex <= endRow && !Context.CancellationTokenSource.IsCancellationRequested; rowIndex++)
                {
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

                    initialValues.Clear();

                    foreach (var kvp in columnIndexes)
                    {
                        var ri = !Transpose ? rowIndex : kvp.Value.Index;
                        var ci = !Transpose ? kvp.Value.Index : rowIndex;

                        var value = GetCellUnmerged(sheet, ri, ci)?.Value;
                        if (TreatEmptyStringAsNull && value != null && (value is string str))
                        {
                            if (AutomaticallyTrimAllStringValues)
                                str = str.Trim();

                            if (string.IsNullOrEmpty(str))
                                str = null;

                            value = str;
                        }

                        value = HandleConverter(value, kvp.Value.Configuration);
                        initialValues.Add(new KeyValuePair<string, object>(kvp.Key, value));
                    }

                    rowCount++;
                    yield return Context.CreateRow(this, initialValues);
                }
            }
            finally
            {
                if (PreLoadedFile == null)
                {
                    package.Dispose();
                }
            }

            Context.RegisterIoCommandSuccess(this, IoCommandKind.fileRead, iocUid, rowCount);
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusExcelReaderFluent
    {
        public static IFluentProcessMutatorBuilder ReadFromExcel(this IFluentProcessBuilder builder, EpPlusExcelReader reader)
        {
            return builder.ReadFrom(reader);
        }
    }
}