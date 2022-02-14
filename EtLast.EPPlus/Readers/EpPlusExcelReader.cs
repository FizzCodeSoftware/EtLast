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
        public IStreamSource Source { get; init; }
        public string SheetName { get; init; }
        public int SheetIndex { get; init; } = -1;

        /// <summary>
        /// Optional, preloaded Excel file. In case this property is provided, the <see cref="Source"/> property is used only for logging purposes.
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

        public Dictionary<string, ReaderColumnConfiguration> Columns { get; init; }
        public ReaderDefaultColumnConfiguration DefaultColumns { get; init; }

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
            if (Source == null)
            {
                if (PreLoadedFile?.File?.Name != null)
                    return Path.GetFileName(PreLoadedFile.File.Name);

                return null;
            }

            if (string.IsNullOrEmpty(SheetName))
                return Source.Topic + "[" + SheetIndex.ToString("D", CultureInfo.InvariantCulture) + "]";
            else
                return Source.Topic + "[" + SheetName + "]";
        }

        protected override void ValidateImpl()
        {
            if (Source == null && PreLoadedFile == null)
                throw new ProcessParameterNullException(this, nameof(Source));

            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (Columns == null)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }

        protected override IEnumerable<IRow> Produce()
        {
            if (Transpose)
            {
                throw new NotImplementedException("Transpose is not finished yet, must be tested before used");
            }

            var columnIndexes = new List<(string rowColumn, int index, ReaderDefaultColumnConfiguration configuration)>();

            NamedStream stream = null;
            var package = PreLoadedFile;

            if (package == null)
            {
                stream = Source.GetStream(this);
                if (stream == null)
                    yield break;

                try
                {
                    package = new ExcelPackage(stream.Stream);
                }
                catch (Exception ex)
                {
                    var exception = new StreamReadException(this, "excel steram read failed", stream, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel stream read failed: {0}, message: {1}",
                        stream.Name, ex.Message));
                    exception.Data.Add("StreamName", stream.Name);

                    Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, null, ex);
                    throw exception;
                }
            }

            // key is the SOURCE column name
            var columnMap = Columns?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

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
                            Source, SheetName, string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", Source);
                        exception.Data.Add("SheetName", SheetName);
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                        Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);
                        throw exception;
                    }
                    else
                    {
                        var exception = new ProcessExecutionException(this, "can't find excel sheet by index");
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't find excel sheet, file name: {0}, sheet index: {1}, existing sheet names: {2}",
                            Source, SheetIndex.ToString("D", CultureInfo.InvariantCulture), string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", Source);
                        exception.Data.Add("SheetIndex", SheetIndex.ToString("D", CultureInfo.InvariantCulture));
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                        Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);
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
                    else if (DefaultColumns != null)
                    {
                        columnIndexes.Add((excelColumn, colIndex, DefaultColumns));
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
                if (stream != null)
                {
                    Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandUid, rowCount);
                    stream.Dispose();
                    stream = null;
                }

                if (PreLoadedFile == null)
                {
                    package.Dispose();
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusExcelReaderFluent
    {
        public static IFluentProcessMutatorBuilder ReadFromExcel(this IFluentProcessBuilder builder, EpPlusExcelReader reader)
        {
            return builder.ReadFrom(reader);
        }
    }
}