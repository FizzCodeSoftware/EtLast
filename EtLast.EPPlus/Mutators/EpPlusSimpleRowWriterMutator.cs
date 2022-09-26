namespace FizzCode.EtLast;

public sealed class EpPlusSimpleRowWriterMutator : AbstractMutator, IRowSink
{
    public ISinkProvider SinkProvider { get; init; }

    public string SheetName { get; init; }

    /// <summary>
    /// Key is the output column title AND the column in the row (later can be customized by setting a <see cref="ExcelColumn"/>).
    /// </summary>
    public Dictionary<string, ExcelColumn> Columns { get; init; }

    public PartitionKeyGenerator PartitionKeyGenerator { get; set; }
    private readonly Dictionary<string, InternalSink> _sinks = new();

    public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; init; }

    private int _rowCounter;

    public EpPlusSimpleRowWriterMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _rowCounter = 0;
    }

    private InternalSink GetSink(string partitionKey)
    {
        var internalKey = partitionKey ?? "\0__nopartition__\0";

        if (_sinks.TryGetValue(internalKey, out var existing))
            return existing;

        var sink = SinkProvider.GetSink(this, partitionKey);
        var package = new ExcelPackage(sink.Stream);
        var workSheet = package.Workbook.Worksheets.Add(SheetName);

        var newSink = new InternalSink()
        {
            Sink = sink,
            Package = package,
            State = new SimpleExcelWriterState()
            {
                Worksheet = workSheet,
                NextRow = 1,
                NextCol = 1,
            },
        };

        foreach (var col in Columns)
        {
            newSink.State.Worksheet.Cells[newSink.State.NextRow, newSink.State.NextCol].Value = col.Key;
            newSink.State.NextCol++;
        }

        newSink.State.NextRow++;
        newSink.State.NextCol = 1;

        _sinks.Add(internalKey, newSink);
        return newSink;
    }

    protected override void CloseMutator()
    {
        foreach (var sink in _sinks.Values)
        {
            if (sink.State.Worksheet != null)
            {
                Finalize?.Invoke(sink.Package, sink.State);
            }

            if (sink.Package != null)
            {
                try
                {
                    sink.Package.Save();
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, sink.Sink.IoCommandUid, _rowCounter);
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, sink.Sink.IoCommandUid, null, ex);
                    throw;
                }

                sink.Package.Dispose();
            }
        }

        if (SinkProvider.AutomaticallyDispose)
        {
            foreach (var sink in _sinks.Values)
            {
                sink.Sink.Stream.Flush();
                sink.Sink.Stream.Close();
                sink.Sink.Stream.Dispose();
            }
        }

        _sinks.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var partitionKey = PartitionKeyGenerator?.Invoke(row, _rowCounter);
        _rowCounter++;

        var sink = GetSink(partitionKey);

        Context.RegisterWriteToSink(row, sink.Sink.SinkUid);

        try
        {
            foreach (var col in Columns)
            {
                var range = sink.State.Worksheet.Cells[sink.State.NextRow, sink.State.NextCol];
                range.Value = row[col.Value?.SourceColumn ?? col.Key];
                if (col.Value?.NumberFormat != null)
                    range.Style.Numberformat.Format = col.Value.NumberFormat;

                sink.State.NextCol++;
            }

            sink.State.NextRow++;
            sink.State.NextCol = 1;
            sink.Sink.IncreaseRowsWritten();
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, sink.Sink.IoCommandKind, sink.Sink.IoCommandUid, sink.Sink.RowsWritten, ex);

            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel sink: {0}", sink.Sink.Name));
            exception.Data.Add("Sink", sink.Sink.Name);
            exception.Data.Add("SheetName", SheetName);
            throw exception;
        }

        yield return row;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (SinkProvider == null)
            throw new ProcessParameterNullException(this, nameof(SinkProvider));

        SinkProvider.Validate(this);

        if (string.IsNullOrEmpty(SheetName))
            throw new ProcessParameterNullException(this, nameof(SheetName));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    private class InternalSink
    {
        public NamedSink Sink { get; init; }
        public ExcelPackage Package { get; init; }
        public SimpleExcelWriterState State { get; init; }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusSimpleRowWriterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder WriteRowToExcelSimple(this IFluentSequenceMutatorBuilder builder, EpPlusSimpleRowWriterMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}