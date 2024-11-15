﻿namespace FizzCode.EtLast;

public sealed class EpPlusPreLoadedSimpleRowWriterMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue]
    public required ExcelPackage PreLoadedFile { get; init; }

    [ProcessParameterMustHaveValue]
    public required string SheetName { get; init; }

    /// <summary>
    /// Key is the output column title AND the column in the row (later can be customized by setting a <see cref="ExcelColumn"/>).
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required Dictionary<string, ExcelColumn> Columns { get; init; }

    public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; init; }

    private SimpleExcelWriterState _state;
    private Sink _sink;
    private string _path;

    protected override void StartMutator()
    {
        _state = new SimpleExcelWriterState();
        _path = PreLoadedFile?.File?.Name ?? "preloaded";
        _sink = Context.GetSink(_path, SheetName, "spreadsheet", this,
            Columns.Select(x => x.Key).ToArray());
    }

    protected override void CloseMutator()
    {
        if (_state.Worksheet != null)
        {
            Finalize?.Invoke(PreLoadedFile, _state);
        }

        _state = null;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        _sink.RegisterRow(row);

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
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel package, file name: {0}, message: {1}, row: {2}", _path, ex.Message, row.ToDebugString()));
            exception.Data["Path"] = _path;
            exception.Data["SheetName"] = SheetName;
            throw exception;
        }

        yield return row;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusPreLoadedSimpleRowWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelSimple(this IFluentSequenceMutatorBuilder builder, EpPlusPreLoadedSimpleRowWriterMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}