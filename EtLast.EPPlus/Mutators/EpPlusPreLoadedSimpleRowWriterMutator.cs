namespace FizzCode.EtLast;

public sealed class EpPlusPreLoadedSimpleRowWriterMutator : AbstractMutator, IRowSink
{
    public ExcelPackage PreLoadedFile { get; init; }
    public string SheetName { get; init; }

    /// <summary>
    /// Key is the output column title AND the column in the row (later can be customized by setting a <see cref="ExcelColumn"/>).
    /// </summary>
    public Dictionary<string, ExcelColumn> Columns { get; init; }

    public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; init; }

    private SimpleExcelWriterState _state;
    private int? _sinkUid;
    private string _fileName;

    public EpPlusPreLoadedSimpleRowWriterMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _state = new SimpleExcelWriterState();
        _fileName = PreLoadedFile?.File?.Name ?? "preloaded";
        _sinkUid ??= Context.GetSinkUid(_fileName, SheetName);
    }

    protected override void CloseMutator()
    {
        if (_state.Worksheet != null)
        {
            Finalize?.Invoke(PreLoadedFile, _state);
        }

        _state = null;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        Context.RegisterWriteToSink(row, _sinkUid.Value);

        _state.Worksheet = PreLoadedFile.Workbook.Worksheets.Add(SheetName);
        _state.NextRow = 1;
        _state.NextCol = 1;
        foreach (var col in Columns)
        {
            _state.Worksheet.Cells[_state.NextRow, _state.NextCol].Value = col.Key;
            _state.NextCol++;
        }

        _state.NextRow++;

        try
        {
            _state.NextCol = 1;
            foreach (var col in Columns)
            {
                var range = _state.Worksheet.Cells[_state.NextRow, _state.NextCol];
                range.Value = row[col.Value?.SourceColumn ?? col.Key];
                if (col.Value?.NumberFormat != null)
                    range.Style.Numberformat.Format = col.Value.NumberFormat;

                _state.NextCol++;
            }

            _state.NextRow++;
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel package", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel package, file name: {0}, message: {1}, row: {2}", _fileName, ex.Message, row.ToDebugString()));
            exception.Data.Add("FileName", _fileName);
            exception.Data.Add("SheetName", SheetName);
            throw exception;
        }

        yield return row;
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

        if (PreLoadedFile == null)
            throw new ProcessParameterNullException(this, nameof(PreLoadedFile));

        if (string.IsNullOrEmpty(SheetName))
            throw new ProcessParameterNullException(this, nameof(SheetName));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusPreLoadedSimpleRowWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelSimple(this IFluentSequenceMutatorBuilder builder, EpPlusPreLoadedSimpleRowWriterMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}