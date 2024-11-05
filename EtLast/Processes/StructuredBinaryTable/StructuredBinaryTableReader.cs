namespace FizzCode.EtLast;

public sealed class StructuredBinaryTableReader : AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required IManyStreamProvider StreamProvider { get; init; }

    /// <summary>
    /// First stream index is (integer) 0
    /// </summary>
    public string AddStreamIndexToColumn { get; init; }

    /// <summary>
    /// First row index is (long) 0
    /// </summary>
    public string AddRowIndexToColumn { get; init; }

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> Produce()
    {
        var rowCount = 0L;

        var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        var addStreamIndex = !string.IsNullOrEmpty(AddStreamIndexToColumn)
            ? AddStreamIndexToColumn
            : null;

        var addRowIndex = !string.IsNullOrEmpty(AddRowIndexToColumn)
            ? AddRowIndexToColumn
            : null;

        var streamIndex = 0;
        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (FlowState.IsTerminating)
                break;

            BinaryReader reader = null;
            try
            {
                reader = new BinaryReader(stream.Stream, Encoding.UTF8, leaveOpen: true);

                var formatVersion = reader.Read7BitEncodedInt();

                var columnCount = reader.Read7BitEncodedInt();
                var columnNames = new string[columnCount];
                var columnTypeNames = new string[columnCount];
                var columnTypeCodes = new BinaryTypeCode[columnCount];
                for (var i = 0; i < columnCount; i++)
                {
                    columnNames[i] = reader.ReadString();
                    columnTypeNames[i] = reader.ReadString();
                    columnTypeCodes[i] = (BinaryTypeCode)reader.ReadByte();
                }

                while (stream.Stream.Position < stream.Stream.Length)
                {
                    for (var i = 0; i < columnCount; i++)
                    {
                        var typeCode = (BinaryTypeCode)reader.ReadByte();
                        if (typeCode != BinaryTypeCode._null)
                        {
                            var value = BinaryTypeCodeEncoder.Read(reader, typeCode);
                            initialValues[columnNames[i]] = value;
                        }
                        else
                        {
                            initialValues[columnNames[i]] = null;
                        }
                    }

                    if (addStreamIndex != null)
                        initialValues[addStreamIndex] = streamIndex;

                    if (addRowIndex != null)
                        initialValues[addRowIndex] = rowCount;

                    rowCount++;
                    yield return Context.CreateRow(this, initialValues);
                    initialValues.Clear();

                    if (FlowState.IsTerminating)
                        break;
                }
            }
            finally
            {
                if (stream != null)
                {
                    stream.IoCommand.AffectedDataCount += rowCount;
                    stream.IoCommand.End();
                    stream.Close();
                    reader?.Dispose();
                }
            }

            streamIndex++;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class StructuredBinaryTableReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadStructuredBinaryTable(this IFluentSequenceBuilder builder, StructuredBinaryTableReader reader)
    {
        return builder.ReadFrom(reader);
    }
}