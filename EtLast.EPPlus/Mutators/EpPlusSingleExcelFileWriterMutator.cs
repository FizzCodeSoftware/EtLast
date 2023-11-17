namespace FizzCode.EtLast;

public sealed class EpPlusSingleExcelFileWriterMutator<TState>(IEtlContext context) : AbstractMutator(context), IRowSink
    where TState : BaseExcelWriterState, new()
{
    [ProcessParameterMustHaveValue]
    public required string FileName { get; init; }

    [ProcessParameterMustHaveValue]
    public required Action<IRow, ExcelPackage, TState> Action { get; init; }

    public Action<ExcelPackage, TState> Initialize { get; init; }
    public Action<ExcelPackage, TState> Finalize { get; init; }
    public ExcelPackage ExistingPackage { get; init; }

    private TState _state;
    private ExcelPackage _package;
    private long? _sinkUid;
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
            var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.fileWrite, Path.GetDirectoryName(FileName), Path.GetFileName(FileName), null, null, null, null,
                "saving excel package", null);

            try
            {
                _package.Save();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, iocUid, _rowCount);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data["FileName"] = FileName;

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, iocUid, null, exception);
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
            _package = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
            Initialize?.Invoke(_package, _state);
        }

        try
        {
            Action.Invoke(row, _package, _state);

            if (_sinkUid != null)
            {
                Context.RegisterWriteToSink(row, _sinkUid.Value);
                _rowCount++;
            }
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}",
                FileName, ex.Message, row.ToDebugString()));
            exception.Data["FileName"] = FileName;
            throw exception;
        }

        yield return row;
    }

    public void AddWorkSheet(string name)
    {
        _state.Worksheet = _package.Workbook.Worksheets.Add(name);
        _sinkUid = Context.GetSinkUid(FileName, name);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusSingleExcelFileWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelFileCustom<TState>(this IFluentSequenceMutatorBuilder builder, EpPlusSingleExcelFileWriterMutator<TState> mutator)
    where TState : BaseExcelWriterState, new()
    {
        return builder.AddMutator(mutator);
    }
}
