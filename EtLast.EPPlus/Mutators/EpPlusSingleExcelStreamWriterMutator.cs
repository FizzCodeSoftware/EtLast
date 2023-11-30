﻿namespace FizzCode.EtLast;

public sealed class EpPlusSingleExcelStreamWriterMutator<TState> : AbstractMutator, IRowSink
    where TState : BaseExcelWriterState, new()
{
    [ProcessParameterMustHaveValue]
    public required string SinkLocation { get; init; }

    [ProcessParameterMustHaveValue]
    public required Stream Stream { get; init; }

    [ProcessParameterMustHaveValue]
    public required Action<IRow, ExcelPackage, TState> Action { get; init; }

    public Action<ExcelPackage, TState> Initialize { get; init; }
    public Action<ExcelPackage, TState> Finalize { get; init; }
    public ExcelPackage ExistingPackage { get; init; }

    private TState _state;
    private ExcelPackage _package;
    private long? _sinkId;
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
            var ioCommand = Context.RegisterIoCommandStart(this, new IoCommand()
            {
                Kind = IoCommandKind.streamWrite,
                Location = Stream.GetType().GetFriendlyTypeName(),
                Message = "saving excel package",
            });

            try
            {
                _package.Save();
                ioCommand.AffectedDataCount += _rowCount;
                Context.RegisterIoCommandEnd(this, ioCommand);
            }
            catch (Exception ex)
            {
                // todo: enrich exception
                ioCommand.Exception = ex;
                Context.RegisterIoCommandEnd(this, ioCommand);
                throw;
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
            _package = ExistingPackage ?? new ExcelPackage(Stream);
            Initialize?.Invoke(_package, _state);
        }

        try
        {
            Action.Invoke(row, _package, _state);

            if (_sinkId != null)
            {
                Context.RegisterWriteToSink(row, _sinkId.Value);
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

    public void AddWorkSheet(string name)
    {
        _state.Worksheet = _package.Workbook.Worksheets.Add(name);
        _sinkId = Context.GetSinkId(SinkLocation, name, "spreadsheet", GetType());
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
