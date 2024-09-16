namespace FizzCode.EtLast;

public sealed class EpPlusSingleExcelFileWriterMutator<TState> : AbstractMutator, IRowSink
    where TState : BaseExcelWriterState, new()
{
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    [ProcessParameterMustHaveValue]
    public required Action<IRow, ExcelPackage, TState> Action { get; init; }

    public Action<ExcelPackage, TState> Initialize { get; init; }
    public Action<ExcelPackage, TState> Finalize { get; init; }
    public ExcelPackage ExistingPackage { get; init; }

    private TState _state;
    private ExcelPackage _package;
    private Sink _sink;
    private long _rowCount;

    protected override void StartMutator()
    {
        _state = new TState();
    }

    protected override void CloseMutator()
    {
        if (_state.Worksheet != null)
        {
            Finalize?.Invoke(_package, _state);
        }

        if (ExistingPackage == null && _package != null)
        {
            var ioCommand = Context.RegisterIoCommand(new IoCommand()
            {
                Process = this,
                Kind = IoCommandKind.fileWrite,
                Location = System.IO.Path.GetDirectoryName(Path),
                Path = System.IO.Path.GetFileName(Path),
                Message = "saving excel package",
            });

            try
            {
                _package.Save();
                ioCommand.AffectedDataCount += _rowCount;
                ioCommand.End();
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}",
                    Path, ex.Message));
                exception.Data["Path"] = Path;

                ioCommand.Failed(exception);
                throw exception;
            }

            _package.Dispose();
            _package = null;
        }

        _state = null;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        if (_package == null) // lazy load here instead of prepare
        {
            _package = ExistingPackage ?? new ExcelPackage(new FileInfo(Path));
            Initialize?.Invoke(_package, _state);
        }

        try
        {
            Action.Invoke(row, _package, _state);

            if (_sink != null)
            {
                _sink.RegisterRow(row);
                _rowCount++;
            }
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}",
                Path, ex.Message, row.ToDebugString()));
            exception.Data["Path"] = Path;
            throw exception;
        }

        yield return row;
    }

    public void AddWorkSheet(string name)
    {
        _state.Worksheet = _package.Workbook.Worksheets.Add(name);
        _sink = Context.GetSink(Path, name, "spreadsheet", this, []);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusSingleExcelFileWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelFileCustom<TState>(this IFluentSequenceMutatorBuilder builder, EpPlusSingleExcelFileWriterMutator<TState> mutator)
    where TState : BaseExcelWriterState, new()
    {
        return builder.AddMutator(mutator);
    }
}
