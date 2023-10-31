namespace FizzCode.EtLast;

public sealed class EpPlusSimpleRowWriterMutator : AbstractMutator, IRowSink
{
    [ProcessParameterNullException]
    public required ISinkProvider SinkProvider { get; init; }

    [ProcessParameterNullException]
    public required string SheetName { get; init; }

    /// <summary>
    /// Key is the output column title AND the column in the row (later can be customized by setting a <see cref="ExcelColumn"/>).
    /// </summary>
    [ProcessParameterNullException]
    public required Dictionary<string, ExcelColumn> Columns { get; init; }

    public PartitionKeyGenerator PartitionKeyGenerator { get; set; }
    public Action<ExcelPackage, SimpleExcelWriterState> Finalize { get; init; }

    private long _rowCounter;
    private readonly Dictionary<string, InternalSink> _sinks = new();

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

        ExcelPackage package;

        var sink = SinkProvider.GetSink(this, partitionKey);
        if (sink.Stream.Length == 0)
        {
            package = new ExcelPackage(sink.Stream);
        }
        else
        {
            var existingContent = new MemoryStream((int)sink.Stream.Length);
            sink.Stream.CopyTo(existingContent, 81920);
            existingContent.Position = 0;

            sink.Stream.SetLength(0);
            package = new ExcelPackage(sink.Stream, existingContent);
        }

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
                    sink.Package.SaveAs(sink.Sink.Stream);
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, sink.Sink.IoCommandUid, _rowCounter);
                }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, "error raised during writing an excel file", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel sink: {0}", sink.Sink.Name));
                    exception.Data["Sink"] = sink.Sink.Name;
                    exception.Data["SheetName"] = SheetName;

                    Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, sink.Sink.IoCommandUid, null, exception);
                    throw exception;
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
            var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel sink: {0}", sink.Sink.Name));
            exception.Data["Sink"] = sink.Sink.Name;
            exception.Data["SheetName"] = SheetName;

            Context.RegisterIoCommandFailed(this, sink.Sink.IoCommandKind, sink.Sink.IoCommandUid, sink.Sink.RowsWritten, exception);
            throw exception;
        }

        yield return row;
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