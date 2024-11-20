using System.Buffers.Binary;
using System.IO.Compression;

namespace FizzCode.EtLast;

public sealed class WriteToStructuredBinaryTableMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue] public required IOneSinkProvider SinkProvider { get; init; }
    [ProcessParameterMustHaveValue] public required Func<Dictionary<string, Type>> DynamicColumns { get; init; }

    /// <summary>
    /// Default value is 10 MB.
    /// </summary>
    public int BufferSizeBytes { get; init; } = 1024 * 1024 * 10;

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
            NamedSink = SinkProvider.GetSink(this, "sbt", ColumnNames),
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
        sinkEntry.NamedSink.Sink.RegisterRow(row);

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

            if (sinkEntry.Buffer.Length >= BufferSizeBytes)
                WriteBuffer();
        }
        catch (Exception ex)
        {
            sinkEntry.NamedSink.IoCommand.AffectedDataCount += sinkEntry.NamedSink.Sink.Rows;
            sinkEntry.NamedSink.IoCommand.Failed(ex);
            throw;
        }

        yield return row;
    }

    private void WriteBuffer()
    {
        if (_sinkEntry.Buffer.Position == 0)
            return;

        _sinkEntry.BufferWriter.Flush();

        using (var targetMs = new MemoryStream())
        {
            using (var ds = new BrotliStream(targetMs, new BrotliCompressionOptions() { Quality = 5 }))
            {
                _sinkEntry.Buffer.Position = 0;
                _sinkEntry.Buffer.CopyTo(ds);
            }

            var data = targetMs.ToArray();

            var buffer = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buffer, data.Length);
            _sinkEntry.NamedSink.Stream.Write(buffer);

            _sinkEntry.NamedSink.Stream.Write(data, 0, data.Length);
            _sinkEntry.NamedSink.Sink.IncreaseBytes(data.Length);
        }

        _sinkEntry.RowCount = 0;
        _sinkEntry.Buffer.SetLength(0);
    }

    private class SinkEntry
    {
        public required NamedSink NamedSink { get; init; }
        public required MemoryStream Buffer { get; init; }
        public required BinaryWriter BufferWriter { get; init; }
        public long RowCount = 0;
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