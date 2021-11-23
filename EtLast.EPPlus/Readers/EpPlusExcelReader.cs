namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using OfficeOpenXml;

    public enum EpPlusExcelHeaderCellMode { Join, KeepFirst, KeepLast }

    public sealed class EpPlusExcelReader : AbstractRowSource
    {
        public string FileName { get; init; }
        public string SheetName { get; init; }
        public int SheetIndex { get; init; } = -1;

        /// <summary>
        /// Optional, preloaded Excel file. In case this property is provided, the FileName property is used only for logging purposes.
        /// Usage example: reader.PreLoadedFile = new ExcelPackage(new FileInfo(fileName));
        /// </summary>
        public ExcelPackage PreLoadedFile { get; init; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; init; } = true;

        /// <summary>
        /// Default true.
        /// </summary>
        public bool AutomaticallyTrimAllStringValues { get; init; } = true;

        public Dictionary<string, ReaderColumnConfiguration> ColumnConfiguration { get; init; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; init; }

        private bool Transpose { get; init; } // todo: implement working transpose

        /// <summary>
        /// Default true.
        /// </summary>
        public bool Unmerge { get; init; } = true;

        public int[] HeaderRows { get; init; } = new[] { 1 };

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

        public EpPlusExcelReader(IEtlContext context)
            : base(context)
        {
        }

        public override string GetTopic()
        {
            if (FileName == null)
                return null;

            if (string.IsNullOrEmpty(SheetName))
                return Path.GetFileName(FileName) + "(" + SheetIndex.ToString("D", CultureInfo.InvariantCulture) + ")";
            else
                return Path.GetFileName(FileName) + "(" + SheetName + ")";
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
                var exception = new FileReadException(this, "input file doesn't exist", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exist: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                throw exception;
            }

            var columnIndexes = new List<(string rowColumn, int index, ReaderDefaultColumnConfiguration configuration)>();

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

                    var exception = new FileReadException(this, "excel file read failed", FileName, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}",
                        FileName, ex.Message));
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }

            // key is the SOURCE column name
            var columnMap = ColumnConfiguration?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

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

                    if (columnMap.TryGetValue(excelColumn, out var columnConfiguration))
                    {
                        columnIndexes.Add((columnConfiguration.rowColumn, colIndex, columnConfiguration.config));
                    }
                    else if (DefaultColumnConfiguration != null)
                    {
                        columnIndexes.Add((excelColumn, colIndex, DefaultColumnConfiguration));
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
                            var ri = !Transpose ? rowIndex : kvp.index;
                            var ci = !Transpose ? kvp.index : rowIndex;

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
                        var ri = !Transpose ? rowIndex : kvp.index;
                        var ci = !Transpose ? kvp.index : rowIndex;

                        var value = GetCellUnmerged(sheet, ri, ci)?.Value;
                        if (TreatEmptyStringAsNull && value != null && (value is string str))
                        {
                            if (AutomaticallyTrimAllStringValues)
                                str = str.Trim();

                            if (string.IsNullOrEmpty(str))
                                str = null;

                            value = str;
                        }

                        value = kvp.configuration.Process(this, value);
                        initialValues.Add(new KeyValuePair<string, object>(kvp.rowColumn, value));
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