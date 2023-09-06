namespace FizzCode.EtLast;

public sealed class WriteToDynamicDelimitedMutator : AbstractMutator, IRowSink
{
    public required ISinkProvider SinkProvider { get; init; }

    /// <summary>
    /// Default value is <see cref="Encoding.UTF8"/>
    /// </summary>
    public required Encoding Encoding { get; init; } = Encoding.UTF8;

    public IValueFormatter ValueFormatter { get; init; } = DelimitedValueFormatter.Default;

    /// <summary>
    /// Default value is <see cref="CultureInfo.InvariantCulture"/>;
    /// </summary>
    public required IFormatProvider FormatProvider { get; init; } = CultureInfo.InvariantCulture;

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
    public required char Delimiter { get; init; } = ';';

    /// <summary>
    /// Default value is true
    /// </summary>
    public required bool WriteHeader { get; init; } = true;

    /// <summary>
    /// Default value is 10000
    /// </summary>
    public int BatchSize { get; init; } = 10000;

    public PartitionKeyGenerator PartitionKeyGenerator { get; init; }

    private readonly Dictionary<string, SinkEntry> _sinkEntries = new();
    private byte[] _delimiterBytes;
    private byte[] _lineEndingBytes;
    private string _escapedQuote;
    private char[] _quoteRequiredChars;
    private string _quoteAsString;

    private int _rowCounter;

    public WriteToDynamicDelimitedMutator(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (SinkProvider == null)
            throw new ProcessParameterNullException(this, nameof(SinkProvider));

        SinkProvider.Validate(this);
    }

    protected override void StartMutator()
    {
        _delimiterBytes = Encoding.GetBytes(new[] { Delimiter });
        _lineEndingBytes = Encoding.GetBytes(LineEnding);
        _escapedQuote = new string(new[] { Escape, Quote });
        _quoteRequiredChars = new[] { Delimiter, Quote, Escape, '\r', '\n' };
        _quoteAsString = Quote.ToString();

        _rowCounter = 0;
    }

    private SinkEntry GetSinkEntry(string partitionKey, IReadOnlySlimRow firstRow)
    {
        var internalKey = partitionKey ?? "\0__nopartition__\0";

        if (_sinkEntries.TryGetValue(internalKey, out var sinkEntry))
            return sinkEntry;

        sinkEntry = new SinkEntry()
        {
            Sink = SinkProvider.GetSink(this, partitionKey),
            Buffer = new MemoryStream(),
            Columns = firstRow.Values.Select(x => x.Key).ToArray(),
        };

        _sinkEntries.Add(internalKey, sinkEntry);

        if (WriteHeader)
        {
            if (sinkEntry.Sink.SafeGetPosition() == 0)
            {
                var first = true;
                foreach (var columnName in sinkEntry.Columns)
                {
                    if (!first)
                        sinkEntry.Sink.Stream.Write(_delimiterBytes);

                    var quoteRequired = !string.IsNullOrEmpty(columnName) &&
                        (columnName.IndexOfAny(_quoteRequiredChars) > -1
                        || columnName[0] == ' '
                        || columnName[^1] == ' '
                        || columnName.Contains(LineEnding, StringComparison.Ordinal));

                    var line = ConvertToDelimitedValue(columnName, quoteRequired);
                    sinkEntry.Sink.Stream.Write(Encoding.GetBytes(line));

                    first = false;
                }

                sinkEntry.Sink.IncreaseRowsWritten();
            }

            sinkEntry.Sink.Stream.Write(_lineEndingBytes);
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
                sinkEntry.Sink.Stream.Flush();
                sinkEntry.Sink.Stream.Close();
                sinkEntry.Sink.Stream.Dispose();
            }
        }

        _sinkEntries.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var partitionKey = PartitionKeyGenerator?.Invoke(row, _rowCounter);
        _rowCounter++;

        var sinkEntry = GetSinkEntry(partitionKey, row);

        Context.RegisterWriteToSink(row, sinkEntry.Sink.SinkUid);

        try
        {
            var first = true;
            foreach (var columnName in sinkEntry.Columns)
            {
                if (!first)
                    sinkEntry.Buffer.Write(_delimiterBytes);

                var value = row[columnName];

                var str = ValueFormatter.Format(value, FormatProvider);
                var quoteRequired = !string.IsNullOrEmpty(str) &&
                    (str.IndexOfAny(_quoteRequiredChars) > -1
                    || str[0] == ' '
                    || str[^1] == ' '
                    || str.Contains(LineEnding, StringComparison.Ordinal));

                var convertedValue = ConvertToDelimitedValue(str, quoteRequired);
                if (convertedValue != null)
                    sinkEntry.Buffer.Write(Encoding.GetBytes(convertedValue));

                first = false;
            }

            sinkEntry.Buffer.Write(_lineEndingBytes);
            sinkEntry.RowCount++;

            if (sinkEntry.RowCount >= BatchSize)
                WriteBuffer(sinkEntry);
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, sinkEntry.Sink.IoCommandKind, sinkEntry.Sink.IoCommandUid, sinkEntry.Sink.RowsWritten, ex);
            throw;
        }

        yield return row;
    }

    private void WriteBuffer(SinkEntry sinkEntry)
    {
        if (sinkEntry.RowCount == 0)
            return;

        var data = sinkEntry.Buffer.ToArray();
        sinkEntry.Sink.Stream.Write(data);
        sinkEntry.Sink.IncreaseRowsWritten(sinkEntry.RowCount);
        sinkEntry.RowCount = 0;
    }

    private string ConvertToDelimitedValue(string value, bool quoteRequired)
    {
        if (quoteRequired)
        {
            if (value != null)
                value = value.Replace(_quoteAsString, _escapedQuote, StringComparison.Ordinal);

            value = Quote + value + Quote;
        }

        return value;
    }

    private class SinkEntry
    {
        public required NamedSink Sink { get; init; }
        public required MemoryStream Buffer { get; init; }
        public int RowCount = 0;
        public required string[] Columns { get; init; }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteToUnstructuredDelimitedMutatorFluent
{
    /// <summary>
    /// Write rows to a delimited stream. The first row if each partition is used to determine the columns of the delimited output.
    /// Because rows doesn't store null values by nature, it is recommended to be sure all values are non-nullable,
    /// or use rows with <see cref="IReadOnlySlimRow.KeepNulls"/> enabled in a sequence which uses this mutator.
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToDynamicDelimited(this IFluentSequenceMutatorBuilder builder, WriteToDynamicDelimitedMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}