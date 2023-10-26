namespace FizzCode.EtLast;

public sealed class EpPlusSingleExcelStreamWriterMutator<TState> : AbstractMutator, IRowSink
    where TState : BaseExcelWriterState, new()
{
    public required string SinkLocation { get; init; }
    public required Stream Stream { get; init; }
    public Action<ExcelPackage, TState> Initialize { get; init; }
    public required Action<IRow, ExcelPackage, TState> Action { get; init; }
    public Action<ExcelPackage, TState> Finalize { get; init; }
    public ExcelPackage ExistingPackage { get; init; }

    private TState _state;
    private ExcelPackage _package;
    private int? _sinkUid;
    private long _rowCount;

    public EpPlusSingleExcelStreamWriterMutator(IEtlContext context)
        : base(context)
    {
    }

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
            var iocUid = Context.RegisterIoCommandStartWithLocation(this, IoCommandKind.streamWrite, Stream.GetType().GetFriendlyTypeName(), null, null, null, null,
                "saving excel package", null);

            try
            {
                _package.Save();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.streamWrite, iocUid, _rowCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.streamWrite, iocUid, null, ex);
                throw;
            }

            _package.Dispose();
            _package = null;
        }

        _state = null;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (_package == null) // lazy load here instead of prepare
        {
            _package = ExistingPackage ?? new ExcelPackage(Stream);
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
            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel stream, message: {0}, row: {1}",
                ex.Message, row.ToDebugString()));

            exception.Data["FileName"] = Stream;
            throw exception;
        }

        yield return row;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (Stream == null)
            throw new ProcessParameterNullException(this, nameof(Stream));

        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));

        if (string.IsNullOrEmpty(SinkLocation))
            throw new ProcessParameterNullException(this, nameof(SinkLocation));
    }

    public void AddWorkSheet(string name)
    {
        _state.Worksheet = _package.Workbook.Worksheets.Add(name);
        _sinkUid = Context.GetSinkUid(SinkLocation, name);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusSingleExcelStreamWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelStreamCustom<TState>(this IFluentSequenceMutatorBuilder builder, EpPlusSingleExcelStreamWriterMutator<TState> mutator)
    where TState : BaseExcelWriterState, new()
    {
        return builder.AddMutator(mutator);
    }
}
