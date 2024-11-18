using System.Buffers.Binary;
using System.IO.Compression;

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

        var lenBuffer = new byte[4];

        var streamIndex = 0;
        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (FlowState.IsTerminating)
                break;

            try
            {
                int columnCount;
                string[] columnNames;
                string[] columnTypeNames;
                BinaryTypeCode[] columnTypeCodes;

                stream.Stream.ReadExactly(lenBuffer, 0, 4);
                var len = BinaryPrimitives.ReadInt32LittleEndian(lenBuffer);
                var buffer = new byte[len];
                stream.Stream.ReadExactly(buffer, 0, len);

                using (var headerUncompressed = new MemoryStream())
                {
                    using (var bufferMs = new MemoryStream(buffer))
                    {
                        using (var ds = new DeflateStream(bufferMs, CompressionMode.Decompress, leaveOpen: true))
                        {
                            ds.CopyTo(headerUncompressed);
                        }
                    }

                    headerUncompressed.Position = 0;

                    var headerReader = new BinaryReader(headerUncompressed, Encoding.UTF8, leaveOpen: false);

                    var formatVersion = headerReader.Read7BitEncodedInt();

                    columnCount = headerReader.Read7BitEncodedInt();
                    columnNames = new string[columnCount];
                    columnTypeNames = new string[columnCount];
                    columnTypeCodes = new BinaryTypeCode[columnCount];
                    for (var i = 0; i < columnCount; i++)
                    {
                        columnNames[i] = headerReader.ReadString();
                        columnTypeNames[i] = headerReader.ReadString();
                        columnTypeCodes[i] = (BinaryTypeCode)headerReader.ReadByte();
                    }

                    headerReader.Dispose();
                }

                while (stream.Stream.Position < stream.Stream.Length)
                {
                    stream.Stream.ReadExactly(lenBuffer, 0, 4);
                    len = BinaryPrimitives.ReadInt32LittleEndian(lenBuffer);
                    buffer = new byte[len];
                    stream.Stream.ReadExactly(buffer, 0, len);

                    using (var ums = new MemoryStream())
                    {
                        using (var bufferMs = new MemoryStream(buffer))
                        using (var ds = new DeflateStream(bufferMs, CompressionMode.Decompress, leaveOpen: true))
                            ds.CopyTo(ums);

                        ums.Position = 0;

                        var reader = new BinaryReader(ums, Encoding.UTF8, leaveOpen: false);
                        try
                        {
                            while (ums.Position < ums.Length)
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
                            reader.Dispose();
                        }
                    }
                }
            }
            finally
            {
                if (stream != null)
                {
                    stream.IoCommand.AffectedDataCount += rowCount;
                    stream.IoCommand.End();
                    stream.Close();
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