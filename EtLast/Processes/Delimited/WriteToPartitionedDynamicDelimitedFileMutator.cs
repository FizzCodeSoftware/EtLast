﻿namespace FizzCode.EtLast;

public sealed class WriteToPartitionedDynamicDelimitedFileMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue]
    public required IPartitionedSinkProvider SinkProvider { get; init; }

    /// <summary>
    /// Default value is <see cref="Encoding.UTF8"/>
    /// </summary>
    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public IValueFormatter ValueFormatter { get; init; } = DelimitedValueFormatter.Default;

    /// <summary>
    /// Default value is <see cref="CultureInfo.InvariantCulture"/>;
    /// </summary>
    public IFormatProvider FormatProvider { get; init; } = CultureInfo.InvariantCulture;

    /// <summary>
    /// Default value is \r\n
    /// </summary>
    public string LineEnding { get; init; } = "\r\n";

    /// <summary>
    /// Default value is "
    /// </summary>
    public char Quote { get; init; } = '\"';

    /// <summary>
    /// Default value is "
    /// </summary>
    public char Escape { get; init; } = '\"';

    /// <summary>
    /// Default value is ';'.
    /// </summary>
    public char Delimiter { get; init; } = ';';

    /// <summary>
    /// Default value is true
    /// </summary>
    public bool WriteHeader { get; init; } = true;

    /// <summary>
    /// Default value is 10000
    /// </summary>
    public int BatchSize { get; init; } = 10000;

    public PartitionKeyGenerator PartitionKeyGenerator { get; init; }

    private readonly Dictionary<string, SinkEntry> _sinkEntries = [];
    private byte[] _delimiterBytes;
    private byte[] _lineEndingBytes;
    private byte[] _quoteBytes;
    private string _escapedQuote;
    private char[] _quoteRequiredChars;
    private string _quoteAsString;

    private int _rowCounter;

    protected override void StartMutator()
    {
        _delimiterBytes = Encoding.GetBytes(Delimiter.ToString());
        _lineEndingBytes = Encoding.GetBytes(LineEnding);
        _escapedQuote = new string([Escape, Quote]);
        _quoteRequiredChars = [Delimiter, Quote, Escape, '\r', '\n'];
        _quoteAsString = Quote.ToString();
        _quoteBytes = Encoding.GetBytes(Quote.ToString());

        _rowCounter = 0;
    }

    private SinkEntry GetSinkEntry(string partitionKey, IReadOnlySlimRow firstRow)
    {
        var internalKey = partitionKey ?? "\0__nopartition__\0";

        if (_sinkEntries.TryGetValue(internalKey, out var sinkEntry))
            return sinkEntry;

        var columns = firstRow.Values.Select(x => x.Key).ToArray();

        sinkEntry = new SinkEntry()
        {
            NamedSink = SinkProvider.GetSink(this, partitionKey, "delimited", columns),
            Buffer = new MemoryStream(),
            Columns = columns,
        };

        _sinkEntries.Add(internalKey, sinkEntry);

        if (WriteHeader)
        {
            if (sinkEntry.NamedSink.SafeGetPosition() == 0)
            {
                var first = true;
                foreach (var columnName in sinkEntry.Columns)
                {
                    if (!first)
                        sinkEntry.NamedSink.Stream.Write(_delimiterBytes);

                    if (!string.IsNullOrEmpty(columnName))
                    {
                        var quoteRequired = columnName.IndexOfAny(_quoteRequiredChars) > -1
                            || columnName[0] == ' '
                            || columnName[^1] == ' '
                            || columnName.Contains(LineEnding, StringComparison.Ordinal);

                        if (quoteRequired)
                        {
                            sinkEntry.NamedSink.Stream.Write(_quoteBytes);

                            if (columnName.Contains(Quote))
                            {
                                sinkEntry.NamedSink.Stream.Write(Encoding.GetBytes(columnName.Replace(_quoteAsString, _escapedQuote, StringComparison.Ordinal)));
                            }
                            else
                            {
                                sinkEntry.NamedSink.Stream.Write(Encoding.GetBytes(columnName));
                            }

                            sinkEntry.NamedSink.Stream.Write(_quoteBytes);
                        }
                        else
                        {
                            sinkEntry.NamedSink.Stream.Write(Encoding.GetBytes(columnName));
                        }
                    }

                    first = false;
                }

                sinkEntry.NamedSink.Stream.Write(_lineEndingBytes);
            }
        }

        return sinkEntry;
    }

    protected override void CloseMutator()
    {
        foreach (var sinkEntry in _sinkEntries.Values)
        {
            WriteBuffer(sinkEntry);
        }

        if (SinkProvider.AutomaticallyDispose)
        {
            foreach (var sinkEntry in _sinkEntries.Values)
            {
                sinkEntry.NamedSink.Stream.Flush();
                sinkEntry.NamedSink.Stream.Close();
                sinkEntry.NamedSink.Stream.Dispose();
            }
        }

        _sinkEntries.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var partitionKey = PartitionKeyGenerator?.Invoke(row, _rowCounter);
        _rowCounter++;

        var sinkEntry = GetSinkEntry(partitionKey, row);
        sinkEntry.NamedSink.Sink.RegisterRow(row);

        try
        {
            var first = true;
            foreach (var columnName in sinkEntry.Columns)
            {
                if (!first)
                    sinkEntry.Buffer.Write(_delimiterBytes);

                var value = row[columnName];

                var str = ValueFormatter.Format(value, FormatProvider);

                if (!string.IsNullOrEmpty(str))
                {
                    var quoteRequired = str.IndexOfAny(_quoteRequiredChars) > -1
                        || str[0] == ' '
                        || str[^1] == ' '
                        || str.Contains(LineEnding, StringComparison.Ordinal);

                    if (quoteRequired)
                    {
                        sinkEntry.Buffer.Write(_quoteBytes);

                        if (str.Contains(Quote))
                        {
                            sinkEntry.Buffer.Write(Encoding.GetBytes(str.Replace(_quoteAsString, _escapedQuote, StringComparison.Ordinal)));
                        }
                        else
                        {
                            sinkEntry.Buffer.Write(Encoding.GetBytes(str));
                        }

                        sinkEntry.Buffer.Write(_quoteBytes);
                    }
                    else
                    {
                        sinkEntry.Buffer.Write(Encoding.GetBytes(str));
                    }
                }

                first = false;
            }

            sinkEntry.Buffer.Write(_lineEndingBytes);
            sinkEntry.RowCount++;

            if (sinkEntry.RowCount >= BatchSize)
                WriteBuffer(sinkEntry);
        }
        catch (Exception ex)
        {
            sinkEntry.NamedSink.IoCommand.AffectedDataCount += sinkEntry.NamedSink.Sink.Rows;
            sinkEntry.NamedSink.IoCommand.Failed(ex);
            throw;
        }

        yield return row;
    }

    private void WriteBuffer(SinkEntry sinkEntry)
    {
        if (sinkEntry.RowCount == 0)
            return;

        var data = sinkEntry.Buffer.ToArray();
        sinkEntry.NamedSink.Stream.Write(data, 0, data.Length);
        sinkEntry.NamedSink.Sink.IncreaseBytes(data.Length);
        sinkEntry.RowCount = 0;
        sinkEntry.Buffer.SetLength(0);
    }

    private class SinkEntry
    {
        public required NamedSink NamedSink { get; init; }
        public required MemoryStream Buffer { get; init; }
        public long RowCount = 0;
        public required string[] Columns { get; init; }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteToPartitionedDynamicDelimitedFileMutatorFluent
{
    /// <summary>
    /// Write rows to a delimited stream. The first row if each partition is used to determine the columns of the delimited output.
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToPartitionedDynamicDelimitedFile(this IFluentSequenceMutatorBuilder builder, WriteToPartitionedDynamicDelimitedFileMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}