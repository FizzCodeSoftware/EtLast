namespace FizzCode.EtLast;

public sealed class WriteToStructuredBinaryTableMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue] public required IOneSinkProvider SinkProvider { get; init; }
    [ProcessParameterMustHaveValue] public required Func<Dictionary<string, Type>> DynamicColumns { get; init; }

    /// <summary>
    /// Default value is 10000
    /// </summary>
    public int BatchSize { get; init; } = 10000;

    private SinkEntry _sinkEntry;

    public static int FormatVersion { get; } = 1;

    private string[] ColumnNames { get; set; }
    private Type[] ColumnTypes { get; set; }
    private BinaryTypeCode[] ColumnTypeCodes { get; set; }

    private SinkEntry GetSinkEntry()
    {
        if (_sinkEntry != null)
            return _sinkEntry;

        var columns = DynamicColumns.Invoke();
        ColumnNames = new string[columns.Count];
        ColumnTypes = new Type[columns.Count];
        ColumnTypeCodes = new BinaryTypeCode[columns.Count];
        var colIdx = 0;
        foreach (var (columnName, columnType) in columns)
        {
            ColumnNames[colIdx] = columnName;
            ColumnTypes[colIdx] = columnType;
            ColumnTypeCodes[colIdx] = BinaryTypeCodeEncoder.GetTypeCode(columnType);
            colIdx++;
        }

        var buffer = new MemoryStream();
        var sinkEntry = _sinkEntry = new SinkEntry()
        {
            NamedSink = SinkProvider.GetSink(this, "dynamicbinarytable", ColumnNames),
            Buffer = buffer,
            BufferWriter = new BinaryWriter(buffer),
        };

        if (sinkEntry.NamedSink.SafeGetPosition() == 0)
        {
            sinkEntry.BufferWriter.Write7BitEncodedInt(FormatVersion);
            sinkEntry.BufferWriter.Write7BitEncodedInt(columns.Count);
            colIdx = 0;
            foreach (var column in columns)
            {
                sinkEntry.BufferWriter.Write(column.Key);
                sinkEntry.BufferWriter.Write(column.Value.FullName);
                sinkEntry.BufferWriter.Write((byte)ColumnTypeCodes[colIdx]);
                colIdx++;
            }

            WriteBuffer();
            sinkEntry.NamedSink.IncreaseRowsWritten();
        }

        return sinkEntry;
    }

    protected override void CloseMutator()
    {
        if (_sinkEntry != null)
        {
            WriteBuffer();

            if (SinkProvider.AutomaticallyDispose)
            {
                _sinkEntry.NamedSink.Stream.Flush();
                _sinkEntry.NamedSink.Stream.Close();
                _sinkEntry.NamedSink.Stream.Dispose();
            }

            _sinkEntry = null;
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var sinkEntry = GetSinkEntry();
        sinkEntry.NamedSink.Sink.RegisterWrite(row);

        try
        {
            for (var i = 0; i < ColumnNames.Length; i++)
            {
                var value = row[ColumnNames[i]];
                if (value != null)
                {
                    var valueType = value.GetType();
                    if (valueType == ColumnTypes[i])
                    {
                        var typeCode = ColumnTypeCodes[i];
                        sinkEntry.BufferWriter.Write((byte)typeCode);
                        BinaryTypeCodeEncoder.Write(sinkEntry.BufferWriter, value, typeCode);
                    }
                    else
                    {
                        var typeCode = BinaryTypeCodeEncoder.GetTypeCode(valueType);
                        sinkEntry.BufferWriter.Write((byte)typeCode);
                        BinaryTypeCodeEncoder.Write(sinkEntry.BufferWriter, value, typeCode);
                    }
                }
                else
                {
                    sinkEntry.BufferWriter.Write((byte)BinaryTypeCode._null);
                }
            }

            sinkEntry.RowCount++;

            if (sinkEntry.RowCount >= BatchSize)
                WriteBuffer();
        }
        catch (Exception ex)
        {
            sinkEntry.NamedSink.IoCommand.AffectedDataCount += sinkEntry.NamedSink.RowsWritten;
            sinkEntry.NamedSink.IoCommand.Failed(ex);
            throw;
        }

        yield return row;
    }

    private void WriteBuffer()
    {
        if (_sinkEntry.RowCount == 0)
            return;

        _sinkEntry.BufferWriter.Flush();

        var data = _sinkEntry.Buffer.ToArray();
        _sinkEntry.NamedSink.Stream.Write(data, 0, data.Length);
        _sinkEntry.NamedSink.IncreaseRowsWritten(_sinkEntry.RowCount);
        _sinkEntry.RowCount = 0;
        _sinkEntry.Buffer.SetLength(0);
    }

    private class SinkEntry
    {
        public required NamedSink NamedSink { get; init; }
        public required MemoryStream Buffer { get; init; }
        public required BinaryWriter BufferWriter { get; init; }
        public int RowCount = 0;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteToDynamicBinaryTableMutatorFluent
{
    /// <summary>
    /// Write rows to a structured binary table (SBT) stream.
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToStructuredBinaryTable(this IFluentSequenceMutatorBuilder builder, WriteToStructuredBinaryTableMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}