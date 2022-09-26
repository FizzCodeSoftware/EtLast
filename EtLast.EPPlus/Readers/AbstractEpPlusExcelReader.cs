namespace FizzCode.EtLast;

public enum EpPlusExcelHeaderCellMode { Join, KeepFirst, KeepLast }

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractEpPlusExcelReader : AbstractRowSource
{
    public string SheetName { get; init; }
    public int SheetIndex { get; init; } = -1;

    /// <summary>
    /// Default true.
    /// </summary>
    public bool TreatEmptyStringAsNull { get; init; } = true;

    /// <summary>
    /// Default true.
    /// </summary>
    public bool AutomaticallyTrimAllStringValues { get; init; } = true;

    public Dictionary<string, ReaderColumn> Columns { get; init; }
    public ReaderDefaultColumn DefaultColumns { get; init; }

    protected bool Transpose { get; init; } // todo: implement working transpose

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

    protected AbstractEpPlusExcelReader(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
            throw new ProcessParameterNullException(this, nameof(SheetName));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    protected IEnumerable<IRow> ProduceFrom(NamedStream stream, ExcelPackage package)
    {
        var name = stream?.Name ?? package.File?.FullName ?? "preloaded";

        if (Transpose)
            throw new NotImplementedException("Transpose is not finished yet, must be tested before used");

        var columnIndexes = new List<(string rowColumn, int index, ReaderDefaultColumn configuration)>();

        // key is the SOURCE column name
        var columnMap = Columns?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

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
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't find excel sheet, stream: {0}, sheet name: {1}, existing sheet names: {2}",
                    name, SheetName, string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                exception.Data.Add("Stream", name);
                exception.Data.Add("SheetName", SheetName);
                exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                if (stream != null)
                    Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);

                throw exception;
            }
            else
            {
                var exception = new ProcessExecutionException(this, "can't find excel sheet by index");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't find excel sheet, stream: {0}, sheet index: {1}, existing sheet names: {2}",
                    name, SheetIndex.ToString("D", CultureInfo.InvariantCulture), string.Join(",", workbook?.Worksheets.Select(x => x.Name))));
                exception.Data.Add("Stream", name);
                exception.Data.Add("SheetIndex", SheetIndex.ToString("D", CultureInfo.InvariantCulture));
                exception.Data.Add("ExistingSheetNames", string.Join(",", workbook?.Worksheets.Select(x => x.Name)));

                if (stream != null)
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

            if (columnMap.TryGetValue(excelColumn, out var column))
            {
                columnIndexes.Add((column.rowColumn, colIndex, column.config));
            }
            else if (DefaultColumns != null)
            {
                columnIndexes.Add((excelColumn, colIndex, DefaultColumns));
            }
        }

        var initialValues = new List<KeyValuePair<string, object>>();

        for (var rowIndex = FirstDataRow; rowIndex <= endRow && !InvocationContext.IsTerminating; rowIndex++)
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

            yield return Context.CreateRow(this, initialValues);
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
